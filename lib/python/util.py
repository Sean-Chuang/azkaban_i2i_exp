from datetime import date, datetime, timedelta

def get_dt(delta, base=None):
    if base is None:
        d = date.today() + timedelta(delta)
    else:
        d = datetime.strptime(base, '%Y-%m-%d') + timedelta(delta)
    return d.strftime('%Y-%m-%d')

