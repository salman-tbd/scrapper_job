"""
API views for the jobs app.
"""

from rest_framework import viewsets, filters, permissions, status as http_status
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models import Q, Count
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from .models import JobPosting, JobScript, JobScheduler
from django.http import StreamingHttpResponse
from django_celery_beat.models import (
    CrontabSchedule,
    IntervalSchedule,
    PeriodicTask,
    SolarSchedule,
    ClockedSchedule,
)
from .serializers import (
    JobPostingListSerializer,
    JobPostingDetailSerializer,
    JobPostingFullSerializer,
    JobScriptListSerializer,
    JobSchedulerListSerializer,
    CrontabScheduleSerializer,
    IntervalScheduleSerializer,
    PeriodicTaskSerializer,
    SolarScheduleSerializer,
    ClockedScheduleSerializer,
)

# Add imports for sync models and serializers
from .models import JobSyncRun, JobSyncPortalResult, JobSyncJobResult
from .serializers import JobSyncRunSerializer, JobSyncPortalResultSerializer, JobSyncJobResultSerializer

# Add imports for job data reception
from .models import Tbl_Machine_Registry, Tbl_Job_Transmission_Log, Tbl_Job_Transmission_Items, Tbl_Node_Users
import json
import logging
import uuid
from datetime import datetime

# Encryption support
try:
    from cryptography.fernet import Fernet
    ENCRYPTION_AVAILABLE = True
except ImportError:
    ENCRYPTION_AVAILABLE = False


class JobPostingViewSet(viewsets.ModelViewSet):
    """
    ViewSet for JobPosting model with external_source filtering.

    Provides:
    - List all jobs with external_source filter
    - Retrieve individual job details
    - External sources listing
    """

    queryset = JobPosting.objects.select_related('company', 'location', 'posted_by').all()
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]

    # Search fields
    search_fields = [
        'title', 'description', 'company__name', 'location__name',
        'location__city', 'location__state', 'tags'
    ]

    # Ordering fields
    ordering_fields = [
        'title', 'scraped_at', 'date_posted', 'salary_min', 'salary_max',
        'company__name', 'location__name'
    ]
    ordering = ['-scraped_at']  # Default ordering

    def get_serializer_class(self):
        """Return serializer. Use full serializer when requested."""
        # Allow ?full=1 to force full serializer for list/detail
        full = self.request.query_params.get('full')
        if full in ('1', 'true', 'True'):
            return JobPostingFullSerializer
        if self.action == 'list':
            return JobPostingListSerializer
        return JobPostingDetailSerializer

    def get_queryset(self):
        """
        Optionally restricts the returned jobs by filtering against
        query parameters in the URL.
        """
        queryset = self.queryset

        # Filter by active status by default, unless explicitly requested
        status_param = self.request.query_params.get('status', None)
        if status_param is None:
            queryset = queryset.filter(status='active')

        # External source filter
        external_source = self.request.query_params.get('external_source', None)
        if external_source:
            queryset = queryset.filter(external_source__icontains=external_source)

        # Month/Year filter (e.g., ?month=9&year=2025). Defaults to current year if only month is provided.
        month_param = self.request.query_params.get('month')
        year_param = self.request.query_params.get('year')
        if month_param:
            try:
                month_int = int(month_param)
                if 1 <= month_int <= 12:
                    from django.utils import timezone
                    year_int = int(year_param) if year_param else timezone.now().year
                    # Filter by date_posted month/year; fallback to scraped_at if date_posted missing
                    queryset = queryset.filter(
                        Q(date_posted__year=year_int, date_posted__month=month_int)
                        | Q(date_posted__isnull=True, scraped_at__year=year_int, scraped_at__month=month_int)
                    )
            except ValueError:
                # Ignore invalid month/year values silently to avoid breaking existing clients
                pass

        return queryset



    @action(detail=False, methods=['get'])
    def external_sources(self, request):
        """Get all external sources with job counts."""
        sources = JobPosting.objects.values('external_source').annotate(
            job_count=Count('id'),
            active_jobs=Count('id', filter=Q(status='active'))
        ).order_by('-active_jobs')

        return Response(list(sources))

    @action(
        detail=False,
        methods=['get'],
        url_path='feed',
        permission_classes=[permissions.AllowAny]
    )
    def feed(self, request):
        """Public, read-only job feed for network sharing.

        Query params:
        - since: ISO8601 datetime (e.g., 2025-09-04T00:00:00Z) or UNIX epoch seconds
        - limit: max items to return (default 100, max 500)
        - offset: pagination offset (default 0)
        - status: filter by status (default 'active')
        - external_source: optional source filter (icontains)
        """
        # Parse 'since'
        since_param = request.query_params.get('since')
        since_dt = None
        if since_param:
            try:
                # Try epoch seconds
                if since_param.isdigit():
                    since_dt = timezone.datetime.fromtimestamp(int(since_param), tz=timezone.utc)
                else:
                    parsed = parse_datetime(since_param)
                    if parsed is not None:
                        since_dt = parsed if timezone.is_aware(parsed) else timezone.make_aware(parsed, timezone.utc)
            except Exception:
                since_dt = None

        # limit/offset with bounds
        try:
            limit = int(request.query_params.get('limit', '100'))
        except ValueError:
            limit = 100
        limit = max(1, min(500, limit))

        try:
            offset = int(request.query_params.get('offset', '0'))
        except ValueError:
            offset = 0
        offset = max(0, offset)

        status_param = request.query_params.get('status')
        external_source = request.query_params.get('external_source')

        qs = JobPosting.objects.select_related('company', 'location', 'posted_by')
        if status_param is None:
            qs = qs.filter(status='active')
        elif status_param:
            qs = qs.filter(status=status_param)
        if external_source:
            qs = qs.filter(external_source__icontains=external_source)
        if since_dt:
            qs = qs.filter(Q(updated_at__gte=since_dt) | Q(scraped_at__gte=since_dt))

        qs = qs.order_by('-updated_at', '-scraped_at')
        items = list(qs[offset:offset + limit])

        def to_feed_item(obj: JobPosting):
            # Salary
            salary_text = obj.salary_raw_text or ''
            if not salary_text:
                try:
                    salary_text = obj.salary_display
                except Exception:
                    salary_text = ''
            # Remote flag
            remote_allowed = False
            try:
                remote_allowed = 'remote' in (obj.work_mode or '').lower()
            except Exception:
                remote_allowed = False
            # Posted date
            posted_dt = obj.date_posted or obj.scraped_at
            posted_iso = posted_dt.isoformat() if posted_dt else None
            return {
                'job_id': str(obj.pk),
                'title': obj.title,
                'company': getattr(obj.company, 'name', ''),
                'location': getattr(obj.location, 'name', '') if obj.location_id else '',
                'description': obj.description or '',
                'salary': salary_text,
                'job_type': obj.job_type or 'full_time',
                'experience_level': obj.experience_level or '',
                'skills': obj.tags_list,
                'skills_text': obj.skills,
                'posted_date': posted_iso,
                'application_url': obj.external_url or '',
                'source_site': obj.external_source or 'scraper',
                'category': obj.job_category or 'other',
                'remote_allowed': remote_allowed,
                'updated_at': obj.updated_at.isoformat() if obj.updated_at else None,
                'preferred_skills': obj.preferred_skills,
                'job_closing_date': obj.job_closing_date,
            }

        data = [to_feed_item(obj) for obj in items]
        return Response({
            'count': len(data),
            'offset': offset,
            'limit': limit,
            'since': since_dt.isoformat() if since_dt else None,
            'server_time': timezone.now().isoformat(),
            'results': data,
        })

    @action(
        detail=False,
        methods=['get'],
        url_path='export',
        permission_classes=[permissions.AllowAny]
    )
    def export(self, request):
        """Stream ALL job data over the network.

        Query params:
        - format: ndjson (default) or json
        - external_source: optional icontains filter
        - status: optional exact match filter (if omitted, includes all statuses)
        """
        fmt = (request.query_params.get('format') or 'ndjson').lower()
        external_source = request.query_params.get('external_source')
        status_param = request.query_params.get('status')

        qs = JobPosting.objects.select_related('company', 'location', 'posted_by')
        if external_source:
            qs = qs.filter(external_source__icontains=external_source)
        if status_param:
            qs = qs.filter(status=status_param)
        qs = qs.order_by('id')  # stable ordering for full export

        def serialize(obj: JobPosting):
            # Salary text
            salary_text = obj.salary_raw_text or ''
            if not salary_text:
                try:
                    salary_text = obj.salary_display
                except Exception:
                    salary_text = ''
            # Remote flag
            remote_allowed = False
            try:
                remote_allowed = 'remote' in (obj.work_mode or '').lower()
            except Exception:
                remote_allowed = False

            return {
                'id': obj.pk,
                'title': obj.title,
                'slug': obj.slug,
                'description': obj.description or '',
                'company': getattr(obj.company, 'name', ''),
                'company_id': getattr(obj.company, 'id', None),
                'location': getattr(obj.location, 'name', '') if obj.location_id else '',
                'location_id': obj.location_id,
                'posted_by': str(getattr(obj.posted_by, 'username', '')),
                'job_category': obj.job_category,
                'job_type': obj.job_type,
                'experience_level': obj.experience_level or '',
                'work_mode': obj.work_mode or '',
                'salary_min': obj.salary_min,
                'salary_max': obj.salary_max,
                'salary_currency': obj.salary_currency,
                'salary_type': obj.salary_type,
                'salary_raw_text': obj.salary_raw_text or '',
                'salary_display': salary_text,
                'external_source': obj.external_source,
                'external_url': obj.external_url,
                'external_id': obj.external_id,
                'status': obj.status,
                'posted_ago': obj.posted_ago or '',
                'date_posted': obj.date_posted.isoformat() if obj.date_posted else None,
                'expired_at': obj.expired_at.isoformat() if obj.expired_at else None,
                'tags': obj.tags or '',
                'tags_list': obj.tags_list,
                'job_closing_date': obj.job_closing_date or '',
                'skills': obj.skills or '',
                'preferred_skills': obj.preferred_skills or '',
                'additional_info': obj.additional_info or {},
                'scraped_at': obj.scraped_at.isoformat() if obj.scraped_at else None,
                'updated_at': obj.updated_at.isoformat() if obj.updated_at else None,
                'remote_allowed': remote_allowed,
            }

        if fmt == 'json':
            import json as _json

            def json_stream():
                yield '['
                first = True
                for obj in qs.iterator(chunk_size=1000):
                    item = serialize(obj)
                    if first:
                        first = False
                    else:
                        yield ','
                    yield _json.dumps(item, ensure_ascii=False)
                yield ']'

            resp = StreamingHttpResponse(json_stream(), content_type='application/json; charset=utf-8')
            resp['Content-Disposition'] = 'attachment; filename="jobs_export.json"'
            return resp

        # Default: NDJSON (one JSON object per line)
        import json as _json

        def ndjson_stream():
            for obj in qs.iterator(chunk_size=1000):
                yield _json.dumps(serialize(obj), ensure_ascii=False) + "\n"

        resp = StreamingHttpResponse(ndjson_stream(), content_type='application/x-ndjson; charset=utf-8')
        resp['Content-Disposition'] = 'attachment; filename="jobs_export.ndjson"'
        return resp


class ReadOnlyListViewSet(viewsets.ReadOnlyModelViewSet):
    """Base class for read-only list/retrieve endpoints."""
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]


class JobScriptViewSet(ReadOnlyListViewSet):
    """List/retrieve JobScript entries."""
    queryset = JobScript.objects.all()
    serializer_class = JobScriptListSerializer
    search_fields = ['name', 'module_path', 'description']
    ordering_fields = ['name', 'created_at', 'updated_at']
    ordering = ['name']


class JobSchedulerViewSet(ReadOnlyListViewSet):
    """List/retrieve JobScheduler entries."""
    queryset = JobScheduler.objects.select_related('script').all()
    serializer_class = JobSchedulerListSerializer
    search_fields = ['script__name', 'frequency', 'day_of_week', 'days_of_month']
    ordering_fields = ['created_at', 'updated_at', 'last_run_at', 'enabled']
    ordering = ['-created_at']


class CrontabScheduleViewSet(ReadOnlyListViewSet):
    queryset = CrontabSchedule.objects.all()
    serializer_class = CrontabScheduleSerializer
    search_fields = ['minute', 'hour', 'day_of_week', 'day_of_month', 'month_of_year', 'timezone']
    ordering_fields = ['id']


class IntervalScheduleViewSet(ReadOnlyListViewSet):
    queryset = IntervalSchedule.objects.all()
    serializer_class = IntervalScheduleSerializer
    search_fields = ['every', 'period']
    ordering_fields = ['every', 'period']


class SolarScheduleViewSet(ReadOnlyListViewSet):
    queryset = SolarSchedule.objects.all()
    serializer_class = SolarScheduleSerializer
    search_fields = ['event', 'latitude', 'longitude']
    ordering_fields = ['id']


class ClockedScheduleViewSet(ReadOnlyListViewSet):
    queryset = ClockedSchedule.objects.all()
    serializer_class = ClockedScheduleSerializer
    search_fields = ['clocked_time']
    ordering_fields = ['clocked_time']


class PeriodicTaskViewSet(ReadOnlyListViewSet):
    queryset = PeriodicTask.objects.select_related('crontab', 'solar', 'clocked').all()
    serializer_class = PeriodicTaskSerializer
    search_fields = ['name', 'task', 'description', 'queue']
    ordering_fields = ['last_run_at', 'total_run_count', 'date_changed', 'enabled']
    ordering = ['-date_changed']


# New read-only viewsets for sync models
class JobSyncRunViewSet(ReadOnlyListViewSet):
    queryset = JobSyncRun.objects.all()
    serializer_class = JobSyncRunSerializer
    search_fields = ['status', 'error_message']
    ordering_fields = ['started_at', 'finished_at', 'jobs_fetched', 'total_synced']
    ordering = ['-started_at']


class JobSyncPortalResultViewSet(ReadOnlyListViewSet):
    queryset = JobSyncPortalResult.objects.select_related('run').all()
    serializer_class = JobSyncPortalResultSerializer
    search_fields = ['portal_name', 'target_url', 'run__id']
    ordering_fields = ['success_count', 'failure_count', 'success_rate', 'batch_size']
    ordering = ['portal_name']


class JobSyncJobResultViewSet(ReadOnlyListViewSet):
    queryset = JobSyncJobResult.objects.select_related('run', 'portal_result').all()
    serializer_class = JobSyncJobResultSerializer
    search_fields = ['job_id', 'request_url', 'error']
    ordering_fields = ['created_at', 'response_status', 'was_success']
    ordering = ['-created_at']


# Job Data Reception API for EvolGroups Integration
logger = logging.getLogger(__name__)

class JobDataReceptionView(APIView):
    """
    Django REST Framework view to receive job data from nodemanager.py
    Following the exact flow pattern from the WhatsApp message system.
    """
    permission_classes = [permissions.AllowAny]  # Allow nodemanager to send data
    
    def __init__(self):
        super().__init__()
        # Use the same encryption key as nodemanager.py
        self.encryption_key = b'PWhqmT8_Tq5HRz5vIsoJBU9gBDOloo1qJG3fyzZOwfM='
        self.fernet = None
        
        if ENCRYPTION_AVAILABLE:
            try:
                self.fernet = Fernet(self.encryption_key)
                logger.info("🔐 Job data reception encryption initialized")
            except Exception as e:
                logger.error(f"❌ Encryption initialization failed: {e}")
                self.fernet = None
    
    def decrypt_data(self, encrypted_data: bytes):
        """Decrypt received data using Fernet encryption."""
        if not self.fernet:
            return None
        
        try:
            decrypted_bytes = self.fernet.decrypt(encrypted_data)
            data = json.loads(decrypted_bytes.decode())
            return data
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return None
    
    def encrypt_response(self, response_data: dict):
        """Encrypt response data for sending back to nodemanager."""
        if not self.fernet:
            return None
        
        try:
            message = json.dumps(response_data, default=str).encode('utf-8')
            encrypted_data = self.fernet.encrypt(message)
            return encrypted_data
        except Exception as e:
            logger.error(f"Response encryption failed: {e}")
            return None
    
    def register_or_update_machine(self, machine_info: dict) -> bool:
        """Register or update machine information in Django database."""
        try:
            machine_id = machine_info.get('machine_id')
            hostname = machine_info.get('host_name', 'unknown')
            username = machine_info.get('windows_login_name', 'unknown')
            access_token = machine_info.get('access_token', '')
            token_secret = machine_info.get('token_secret', '')
            
            # Get or create machine registry
            machine, created = Tbl_Machine_Registry.objects.get_or_create(
                machine_id=machine_id,
                defaults={
                    'hostname': hostname,
                    'username': username,
                    'access_token': access_token,
                    'token_secret': token_secret,
                    'total_transmissions': 1,
                    'successful_transmissions': 0,
                    'is_authorized': True
                }
            )
            
            if not created:
                # Update existing machine
                machine.hostname = hostname
                machine.username = username
                machine.access_token = access_token
                machine.token_secret = token_secret
                machine.total_transmissions += 1
                machine.last_seen = timezone.now()
                machine.save()
                
                logger.info(f"🔄 Updated existing machine: {hostname} ({username})")
            else:
                logger.info(f"✨ Registered new machine: {hostname} ({username})")
            
            return machine
            
        except Exception as e:
            logger.error(f"Failed to register/update machine: {e}")
            return None
    
    def store_job_data(self, machine, job_data: dict, transmission_log) -> int:
        """Store received job data in Django database."""
        try:
            stored_count = 0
            
            # Store new jobs
            new_jobs = job_data.get('new_jobs', [])
            for job_data_item in new_jobs:
                try:
                    # Get the original job posting if it exists
                    job_id = job_data_item.get('job_id')
                    if job_id:
                        try:
                            job_posting = JobPosting.objects.get(id=job_id)
                            
                            # Create transmission item record
                            Tbl_Job_Transmission_Items.objects.create(
                                transmission_log=transmission_log,
                                job_posting=job_posting,
                                item_type='new_job',
                                was_successful=True,
                                payload_data=job_data_item
                            )
                            stored_count += 1
                            
                        except JobPosting.DoesNotExist:
                            logger.warning(f"Job ID {job_id} not found in database")
                            
                except Exception as item_error:
                    logger.error(f"Failed to store individual job: {item_error}")
            
            # Handle job updates
            job_updates = job_data.get('job_updates', [])
            for update_item in job_updates:
                try:
                    job_id = update_item.get('job_id')
                    if job_id:
                        try:
                            job_posting = JobPosting.objects.get(id=job_id)
                            
                            # Create transmission item record for update
                            Tbl_Job_Transmission_Items.objects.create(
                                transmission_log=transmission_log,
                                job_posting=job_posting,
                                item_type='job_update',
                                was_successful=True,
                                payload_data=update_item
                            )
                            
                        except JobPosting.DoesNotExist:
                            logger.warning(f"Job ID {job_id} not found for update")
                            
                except Exception as update_error:
                    logger.error(f"Failed to store job update: {update_error}")
            
            logger.info(f"💾 Stored {stored_count} new jobs and {len(job_updates)} updates from machine {machine.hostname}")
            return stored_count
            
        except Exception as e:
            logger.error(f"Failed to store job data: {e}")
            return 0
    
    def get(self, request):
        """Handle GET requests - return server status."""
        try:
            total_machines = Tbl_Machine_Registry.objects.count()
            total_transmissions = Tbl_Job_Transmission_Log.objects.count()
            
            return Response({
                'status': 'ready',
                'message': 'Django Job Data Reception Server is ready',
                'server_time': timezone.now().isoformat(),
                'total_machines': total_machines,
                'total_transmissions': total_transmissions,
                'encryption_available': ENCRYPTION_AVAILABLE
            })
        except Exception as e:
            logger.error(f"Error in GET request: {e}")
            return Response(
                {'error': str(e)},
                status=http_status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def post(self, request):
        """Handle POST requests - receive job data from nodemanager.py."""
        try:
            # Get request data
            request_data = request.body
            content_type = request.META.get('CONTENT_TYPE', 'application/json')
            
            logger.info(f"📥 Received job data transmission")
            logger.info(f"   Content-Type: {content_type}")
            logger.info(f"   Data size: {len(request_data)} bytes")
            
            if not request_data:
                return Response(
                    {'error': 'No data received'},
                    status=http_status.HTTP_400_BAD_REQUEST
                )
            
            # Determine if data is encrypted and decrypt
            is_encrypted = content_type == 'application/octet-stream'
            
            if is_encrypted and self.fernet:
                data = self.decrypt_data(request_data)
                if not data:
                    return Response(
                        {'error': 'Decryption failed'},
                        status=http_status.HTTP_400_BAD_REQUEST
                    )
                logger.info("   🔓 Successfully decrypted data")
            else:
                # Handle as JSON
                try:
                    data = json.loads(request_data.decode('utf-8'))
                    logger.info("   📄 Processing JSON data")
                except json.JSONDecodeError as e:
                    return Response(
                        {'error': f'Invalid JSON data: {e}'},
                        status=http_status.HTTP_400_BAD_REQUEST
                    )
            
            # Extract components
            machine_info = data.get('machine_info', {})
            job_data = data.get('job_data', {})
            system_stats = data.get('system_stats', {})
            
            machine_id = machine_info.get('machine_id')
            if not machine_id:
                return Response(
                    {'error': 'Missing machine_id'},
                    status=http_status.HTTP_400_BAD_REQUEST
                )
            
            logger.info(f"   🖥️ Machine: {machine_info.get('host_name')} ({machine_info.get('windows_login_name')})")
            logger.info(f"   🔑 Machine ID: {machine_id}")
            
            # Register/update machine
            machine = self.register_or_update_machine(machine_info)
            if not machine:
                return Response(
                    {'error': 'Machine registration failed'},
                    status=http_status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            # Create transmission log
            transmission_id = str(uuid.uuid4())[:12]
            new_jobs = job_data.get('new_jobs', [])
            job_updates = job_data.get('job_updates', [])
            
            transmission_log = Tbl_Job_Transmission_Log.objects.create(
                machine=machine,
                transmission_id=transmission_id,
                jobs_sent=len(new_jobs),
                updates_sent=len(job_updates),
                total_payload_size=len(request_data),
                status='in_progress',
                was_encrypted=is_encrypted,
                encryption_method='Fernet' if is_encrypted else 'None'
            )
            
            # Store job data
            stored_count = self.store_job_data(machine, job_data, transmission_log)
            
            # Update transmission log
            transmission_log.status = 'success'
            transmission_log.completed_at = timezone.now()
            transmission_log.save()
            
            # Update machine success count
            machine.successful_transmissions += 1
            machine.last_transmission_status = 'success'
            machine.save()
            
            # Prepare response
            response_data = {
                'status': 'success',
                'message': f'Successfully processed {len(new_jobs)} jobs and {len(job_updates)} updates',
                'transmission_id': transmission_id,
                'jobs_processed': len(new_jobs),
                'updates_processed': len(job_updates),
                'jobs_stored': stored_count,
                'server_time': timezone.now().isoformat(),
                'machine_stats': {
                    'total_transmissions': machine.total_transmissions,
                    'successful_transmissions': machine.successful_transmissions,
                    'success_rate': machine.success_rate,
                    'last_seen': machine.last_seen.isoformat() if machine.last_seen else None
                }
            }
            
            logger.info(f"   ✅ Successfully processed transmission {transmission_id}")
            
            # Try to encrypt response if client sent encrypted data
            if is_encrypted and self.fernet:
                encrypted_response = self.encrypt_response(response_data)
                if encrypted_response:
                    from django.http import HttpResponse
                    response = HttpResponse(
                        encrypted_response,
                        content_type='application/octet-stream'
                    )
                    return response
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"❌ Error processing job data: {e}")
            return Response(
                {'error': str(e)},
                status=http_status.HTTP_500_INTERNAL_SERVER_ERROR
            )


@api_view(['GET'])
@permission_classes([permissions.AllowAny])
def job_data_stats(request):
    """Get job data reception statistics."""
    try:
        total_machines = Tbl_Machine_Registry.objects.count()
        total_transmissions = Tbl_Job_Transmission_Log.objects.count()
        successful_transmissions = Tbl_Job_Transmission_Log.objects.filter(status='success').count()
        
        # Get recent transmissions
        recent_transmissions = Tbl_Job_Transmission_Log.objects.select_related('machine').order_by('-started_at')[:10]
        
        recent_data = []
        for transmission in recent_transmissions:
            recent_data.append({
                'transmission_id': transmission.transmission_id,
                'machine_hostname': transmission.machine.hostname,
                'machine_username': transmission.machine.username,
                'jobs_sent': transmission.jobs_sent,
                'updates_sent': transmission.updates_sent,
                'status': transmission.status,
                'started_at': transmission.started_at.isoformat(),
                'was_encrypted': transmission.was_encrypted
            })
        
        return Response({
            'total_machines': total_machines,
            'total_transmissions': total_transmissions,
            'successful_transmissions': successful_transmissions,
            'success_rate': (successful_transmissions / total_transmissions * 100) if total_transmissions > 0 else 0,
            'recent_transmissions': recent_data,
            'server_time': timezone.now().isoformat()
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)},
            status=http_status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([permissions.AllowAny])
def registered_machines(request):
    """Get list of registered machines."""
    try:
        machines = Tbl_Machine_Registry.objects.all().order_by('-last_seen')
        
        machines_data = []
        for machine in machines:
            machines_data.append({
                'machine_id': machine.machine_id,
                'hostname': machine.hostname,
                'username': machine.username,
                'is_authorized': machine.is_authorized,
                'total_transmissions': machine.total_transmissions,
                'successful_transmissions': machine.successful_transmissions,
                'success_rate': machine.success_rate,
                'first_seen': machine.first_seen.isoformat() if machine.first_seen else None,
                'last_seen': machine.last_seen.isoformat() if machine.last_seen else None,
                'last_transmission_status': machine.last_transmission_status
            })
        
        return Response({
            'machines': machines_data,
            'total_count': len(machines_data)
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)},
            status=http_status.HTTP_500_INTERNAL_SERVER_ERROR
        )
