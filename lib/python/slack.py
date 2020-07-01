from slacker import Slacker
import smartcommon

SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'
DMP_AZKABAN_CHANNEL = '#ad-dmp-azkaban'

slack = Slacker('', incoming_webhook_url=SLACK_WEBHOOK_URL)

logger = smartcommon.get_logger("smartnews.slack")


def notify(text, channel=DMP_AZKABAN_CHANNEL, attachments=None):
    slack.incomingwebhook.post({
        'channel': channel,
        'text': text,
        'link_names': '1',
        'attachments': attachments
    })


def print_df(df, channel="#ad-dmp-azkaban-dev", title="Untitled", author="df_reporter", color="good"):
    cols = list(df.columns.values)
    attachments = []
    logger.info(df)
    for idx, row in df.iterrows():
        fs = []
        for col in cols:
            fs.append({
                "title": col,
                "value": row[col],
                "short": True,
            })
        attachments.append({
            "fallback": title,
            "color": color,
            "author_name": author,
            "title": "%s [%02d]" % (title, idx),
            "fields": fs
        })

    slack.incomingwebhook.post({
        'channel': channel,
        'link_names': '1',
        'attachments': attachments
    })
