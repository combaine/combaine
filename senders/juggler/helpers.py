
import datetime


def in_time_of_day(bot, top):
    cur_value = datetime.datetime.now().hour
    if bot < top:
        return bot <= cur_value <= top
    else:
        return bot <= cur_value or cur_value < top


def iftimeofday(bot, top, in_val, out_val):
    return in_val if in_time_of_day(bot, top) else out_val


def ifelse(cond, true, false):
    return true if cond else false


helpers_globals = {
    "iftimeofday": iftimeofday,
    "ifelse": ifelse,
}


if __name__ == '__main__':
    r = "iftimeofday(bot, top, 2, 3)"
    assert eval(r, None, {'iftimeofday': iftimeofday, 'bot': 20, 'top': 13}) == 2
