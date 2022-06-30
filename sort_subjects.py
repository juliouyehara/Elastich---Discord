from utils.discord import post_discord
import time

def sort_by_subject(elastic, index, key):
    email_list = {}
    elastich_control = True
    while elastich_control is True:
        try:
            data = elastic.get_data(query="*", index=index)
            elastich_control = False
        except Exception as e:
            print(e)
    elastich_control = True
    for d in data:
        subject = d['_source']['subject']
        if subject in email_list.keys():
            email_list[subject] += 1
        else:
            email_list.update({subject: 1})
    f = f"{'-' * 80}\n"
    msg = f'{f}MARKETPLACE: {(index.split("_")[0]).upper()}\n{f}'
    post_discord(url=key, msg=msg)
    time.sleep(1)
    for k, v in email_list.items():
        msg = f'{k}: {v}\n'
        post_discord(url=key, msg=msg)
        time.sleep(1.5)

    return msg
