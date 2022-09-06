def time_to_lifetime(time):
    t = str(time)
    ms = int(t[-3:])
    s = int(t[-5:-3])
    sc = int(t[-7:-5])
    h = int(t[:-7])
    t = (h * 3600 + sc * 60 + s) * 1000 + ms
    return t