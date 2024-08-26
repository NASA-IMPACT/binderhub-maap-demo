from maap.maap import MAAP
from datetime import timedelta, datetime, timezone
maap = MAAP(maap_host='api.maap-project.org')
jobs = maap.listJobs(username='gcorradini')
jobs = jobs.json()

now_utc = datetime.now(timezone.utc)
def filter_jobs(jobs, status, past_hours):
    now_utc = datetime.now(timezone.utc)

    def job_within_last_hours(job_dict):
        job_value_dict = list(job_dict.values())[0]
        if job_value_dict['status'] != status:
            return False

        time_end_iso = job_value_dict['job']['job_info']['time_end']
        time_end_dt = datetime.fromisoformat(time_end_iso.replace('Z', '+00:00')).astimezone(timezone.utc)
        return abs(now_utc - time_end_dt) <= timedelta(hours=past_hours)

    filtered_jobs = [
        [job_value_dict['payload_id'], job_value_dict['job']['params']['_command'], job_value_dict['job']['tag'], job_value_dict['job']['job_info']['metrics']['products_staged'][0]['urls'][0]]
        for job_dict in jobs
        if (job_value_dict := list(job_dict.values())[0]) and job_within_last_hours(job_dict)
    ]

    return sorted(filtered_jobs, key=lambda l: l[0])


jobs = filter_jobs(jobs['jobs'], 'job-completed', 24)
print(f"[ FOUND ]: {len(jobs)} within 24 hours")
job = jobs
latest = job[0]
print(f"[ LATEST ]: {latest}")
print(f"[ S3 OUTPUTS ]: {latest[3]}")

