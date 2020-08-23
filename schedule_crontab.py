"""
Use python crontab wrapper to schedule algotrade.py to run every 5 minutes
"""

from crontab import CronTab

my_cron = CronTab(user='standentrading')

# what jobs are set to run
for job in my_cron:
    print(job)

# add a new job
job = my_cron.new(command='python3 algotrade.py')
job.minute.every(1)
my_cron.write()
