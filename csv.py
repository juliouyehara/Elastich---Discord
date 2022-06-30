import pandas as pd
from io import StringIO


def convert_to_csv(emails):
    buffer = StringIO()
    df = pd.DataFrame(emails)
    df.to_csv(buffer)
    return buffer.getvalue()
