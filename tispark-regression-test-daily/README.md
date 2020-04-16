# TiSpark Regression Test Daily
The test will be triggled by jenkins every day.

The test result will be send to Slack(url=pingcap.slack.com, channel=tispark-daily-test).

The following test is or will be included in the daily test:

| tidb/tikv/pd version | spark test version | test |
| -------------------- | ------------------ | ---- |
| master               | 2.4.3              | yes  |
| v4.0.0-rc            | 2.4.3              | yes  |
| v3.1.0-rc            | 2.4.3              | yes  |
| v3.0.12              | 2.4.3              | yes  |
| v2.1.19              | 2.4.3              | yes  |
