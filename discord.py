from discord_webhook import DiscordWebhook



def post_discord (url, msg):
    webhook = DiscordWebhook( url = url ,  content = msg )
    webhook.execute()
