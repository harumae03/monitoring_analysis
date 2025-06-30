# Analysis

Regarding monitoring login data, here is my plan for using baseline.csv (normal activity) and measured.csv (current activity) to address the “real issues” you mentioned without sending too many alerts for small things.

First, I look at the baseline.csv file. This tells us what is normal for logins. Since login numbers change depending on the time of day and day of the week I use this source data to calculate the normal login count for each minute of a normal week. I also calculate how much that number typically varies for any given minute.

Then, when we look at the current data in the measured.csv file I compare the number of logins each minute to what we expect for the exact same minute (like Tuesday at 9:05am) based on the baseline pattern. We only want alerts for “real problems,” so that the alert only occurs when something strange happens and stays that way for a few minutes in a row (we can decide together whether 5 or 10 minutes is right). This means we’re looking for two main things:

Login count drops a lot: If the login count drops well below the expected normal range for that particular time and stays low for those consecutive minutes. This could indicate that the service is down or there’s some other issue. The exception is seeing zero logins when we would normally expect some activity. This is a strong signal that something is wrong.

Login count is high: If the login count jumps much higher than usual for that time and stays high for those consecutive minutes. This could indicate that something unusual is happening, maybe a security issue or unexpected load that needs to be checked.

The idea is to filter out short-term, random changes. You will receive a notification when a potential issue appears to be starting (based on it lasting a few minutes), and another notification when activity returns to a normal range for a similar duration, meaning the issue appears to be resolved.

Some things to consider:

Not enough data for baseline: Baseline.csv is only one week. We should make sure that this week truly represents “normal.” It may not include things like holidays, or perhaps the company has grown since then. We may need more data for the baseline later or update it periodically.

What is considered “significant”? We need to agree on how much lower or higher the number of logins should be to trigger an alert, and how many minutes it should last. We can start with some reasonable estimates based on the normal variation in the data, and then adjust these thresholds based on what events actually require attention.

What this method catches: This approach is good for detecting unusual login counts (too high or too low/zero). This may not catch all possible problems.
