from job_service_mock import JobService


def test_tenacity_retry():
    target_count = 3
    JobService.tenacity_example(target_count)
    retry_count = JobService.tenacity_example.retry.statistics["attempt_number"]
    assert retry_count == target_count
