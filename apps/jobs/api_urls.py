"""
API URL configuration for jobs app.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .api_views import (
    JobPostingViewSet,
    JobScriptViewSet,
    JobSchedulerViewSet,
    CrontabScheduleViewSet,
    IntervalScheduleViewSet,
    PeriodicTaskViewSet,
    SolarScheduleViewSet,
    ClockedScheduleViewSet,
)
# Add new imports for sync model viewsets
from .api_views import JobSyncRunViewSet, JobSyncPortalResultViewSet, JobSyncJobResultViewSet

# Add new imports for job data reception
from .api_views import JobDataReceptionView, job_data_stats, registered_machines

router = DefaultRouter()
router.register(r'jobs', JobPostingViewSet, basename='job')
router.register(r'job-scripts', JobScriptViewSet, basename='jobscript')
router.register(r'job-schedulers', JobSchedulerViewSet, basename='jobscheduler')
router.register(r'beat/crontabs', CrontabScheduleViewSet, basename='beat-crontab')
router.register(r'beat/intervals', IntervalScheduleViewSet, basename='beat-interval')
router.register(r'beat/periodic-tasks', PeriodicTaskViewSet, basename='beat-periodic-task')
router.register(r'beat/solar-events', SolarScheduleViewSet, basename='beat-solar')
router.register(r'beat/clocked', ClockedScheduleViewSet, basename='beat-clocked')
# Register routes for sync models
router.register(r'job-sync/runs', JobSyncRunViewSet, basename='job-sync-run')
router.register(r'job-sync/portal-results', JobSyncPortalResultViewSet, basename='job-sync-portal-result')
router.register(r'job-sync/job-results', JobSyncJobResultViewSet, basename='job-sync-job-result')

urlpatterns = [
    path('', include(router.urls)),
    
    # Job Data Reception Endpoints (for nodemanager.py integration)
    path('receive-job-data/', JobDataReceptionView.as_view(), name='receive-job-data'),
    path('job-data-stats/', job_data_stats, name='job-data-stats'),
    path('registered-machines/', registered_machines, name='registered-machines'),
]
