import importlib
import logging
from typing import Callable

from celery import shared_task
from django.utils import timezone
from asgiref.sync import async_to_sync, sync_to_async

from .models import JobScheduler

logger = logging.getLogger(__name__)


def _import_callable(path: str) -> Callable:
    if ':' in path:
        module_path, attr = path.split(':', 1)
    else:
        # Support dotted path to a callable named `run`
        module_path, attr = path, 'run'
    module = importlib.import_module(module_path)
    func = getattr(module, attr)
    if not callable(func):
        raise TypeError(f"Target {path} is not callable")
    return func


def _load_scheduler_data(scheduler_id: int):
    """Fetch scheduler-related data in a sync context.

    Returns a dict with required fields or None if missing.
    """
    try:
        scheduler = JobScheduler.objects.select_related('script').get(id=scheduler_id)
    except JobScheduler.DoesNotExist:
        return None
    return {
        'enabled': scheduler.enabled,
        'script_is_active': scheduler.script.is_active,
        'module_path': scheduler.script.module_path,
        'source_name': scheduler.source_name,  # Include source for JobIngestionSummary
    }


def _update_last_run_timestamp(scheduler_id: int):
    """Update last_run_at for the given scheduler in a sync context."""
    JobScheduler.objects.filter(id=scheduler_id).update(last_run_at=timezone.now())


def _create_ingestion_summary_on_start(source: str, started_at):
    """Create JobIngestionSummary record when execution STARTS."""
    from .models import JobIngestionSummary
    from datetime import date
    
    if not source or source == 'unknown':
        logger.debug("No source provided, skipping JobIngestionSummary creation")
        return
    
    today = date.today()
    
    try:
        # Create or get existing record
        summary, created = JobIngestionSummary.objects.get_or_create(
            summary_date=today,
            source=source,
            defaults={
                'execution_started_at': started_at,
                'execution_finished_at': None,
                'status': 'running'
            }
        )
        
        if not created:
            # Update existing record with new start time
            summary.execution_started_at = started_at
            summary.execution_finished_at = None
            summary.status = 'running'
            summary.save(update_fields=['execution_started_at', 'execution_finished_at', 'status'])
        
        logger.info(
            "📊 Created JobIngestionSummary for %s: Started at %s",
            source,
            started_at.strftime('%H:%M:%S')
        )
    except Exception as e:
        logger.error(f"Failed to create JobIngestionSummary: {e}")


def _update_ingestion_summary_on_finish(
    source: str,
    started_at,
    finished_at,
    status='success',
    error_message=''
):
    """Update JobIngestionSummary when execution FINISHES."""
    from .models import JobIngestionSummary
    from datetime import date
    
    if not source or source == 'unknown':
        logger.debug("No source provided, skipping JobIngestionSummary update")
        return
    
    today = date.today()
    
    try:
        # Update the existing record
        summary = JobIngestionSummary.objects.get(
            summary_date=today,
            source=source
        )
        
        # Update with finish time and status
        summary.execution_finished_at = finished_at
        summary.status = status
        if error_message:
            summary.error_log = error_message
        summary.save(update_fields=['execution_finished_at', 'status', 'error_log'])
        
        duration = finished_at - started_at
        logger.info(
            "✅ Updated JobIngestionSummary for %s: Finished at %s (duration: %s)",
            source,
            finished_at.strftime('%H:%M:%S'),
            duration
        )
    except JobIngestionSummary.DoesNotExist:
        # If record doesn't exist (shouldn't happen), create it
        logger.warning(f"JobIngestionSummary not found for {source}, creating new record")
        JobIngestionSummary.objects.create(
            summary_date=today,
            source=source,
            execution_started_at=started_at,
            execution_finished_at=finished_at,
            status=status,
            error_log=error_message
        )
    except Exception as e:
        logger.error(f"Failed to update JobIngestionSummary: {e}")


@shared_task(bind=True, name='jobs.execute_script')
def execute_script(self, scheduler_id: int) -> dict:
    """Execute the configured scraper callable and record run metadata."""
    data = async_to_sync(sync_to_async(_load_scheduler_data, thread_sensitive=True))(scheduler_id)
    if data is None:
        logger.warning("Scheduler %s no longer exists; skipping", scheduler_id)
        return {'skipped': True, 'reason': 'scheduler_missing'}
    if not data['enabled'] or not data['script_is_active']:
        logger.info("Scheduler %s disabled or script inactive; skipping", scheduler_id)
        return {'skipped': True}

    target_path = data['module_path']
    source_name = data.get('source_name', '')
    
    # 🆕 CAPTURE START TIME
    execution_started_at = timezone.now()
    
    # 🆕 CREATE RECORD IMMEDIATELY WHEN EXECUTION STARTS
    if source_name:
        async_to_sync(sync_to_async(_create_ingestion_summary_on_start, thread_sensitive=True))(
            source=source_name,
            started_at=execution_started_at
        )
    
    try:
        func = _import_callable(target_path)
        logger.info("🚀 Executing scraper: %s (started at %s)", target_path, execution_started_at.strftime('%H:%M:%S'))
        
        # Execute the scraper
        result = func()
        
        # 🆕 CAPTURE END TIME
        execution_finished_at = timezone.now()
        duration = execution_finished_at - execution_started_at
        
        # Update scheduler's last_run_at
        async_to_sync(sync_to_async(_update_last_run_timestamp, thread_sensitive=True))(scheduler_id)
        
        # 🆕 UPDATE JOB INGESTION SUMMARY WITH FINISH TIME
        if source_name:
            async_to_sync(sync_to_async(_update_ingestion_summary_on_finish, thread_sensitive=True))(
                source=source_name,
                started_at=execution_started_at,
                finished_at=execution_finished_at,
                status='success'
            )
        
        logger.info("✅ Scraper completed: %s (duration: %s)", target_path, duration)
        
        return {
            'ok': True,
            'result': result,
            'execution_started_at': execution_started_at.isoformat(),
            'execution_finished_at': execution_finished_at.isoformat(),
            'duration_seconds': duration.total_seconds()
        }
        
    except Exception as exc:
        # 🆕 CAPTURE END TIME EVEN ON FAILURE
        execution_finished_at = timezone.now()
        logger.exception("❌ Error executing scraper %s", target_path)
        
        # 🆕 UPDATE WITH FAILURE STATUS
        if source_name:
            async_to_sync(sync_to_async(_update_ingestion_summary_on_finish, thread_sensitive=True))(
                source=source_name,
                started_at=execution_started_at,
                finished_at=execution_finished_at,
                status='failed',
                error_message=str(exc)
            )
        
        return {
            'ok': False,
            'error': str(exc),
            'execution_started_at': execution_started_at.isoformat(),
            'execution_finished_at': execution_finished_at.isoformat()
        }


