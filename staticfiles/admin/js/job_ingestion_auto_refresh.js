/**
 * Auto-refresh Job Ingestion Summary page every 30 seconds
 * This ensures the "execution_finished_at" field updates automatically
 * without manual page reload when Docker ETL jobs complete.
 */
(function() {
    // Only run on the JobIngestionSummary changelist page
    if (window.location.pathname.includes('/jobs/jobingestionsummary/')) {
        // Refresh every 30 seconds
        setTimeout(function() {
            window.location.reload();
        }, 30000); // 30000ms = 30 seconds
    }
})();

