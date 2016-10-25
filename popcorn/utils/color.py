normal = 0
bright = 1
_black_fg = 30
_black_bg = _black_fg + 10 
_red_fg = 31
_red_bg = _red_fg + 10
_green_fg = 32
_green_bg = _green_fg + 10
_yellow_fg = 33
_yello_bg = _yellow_fg + 10
_blue_fg = 34
_blue_bg = _blue_fg + 10
_pink_fg = 35
_pink_bg = _pink_fg + 10
_white_fg = 37
_white_bg = _white_fg + 10


def red(input):
    format = ';'.join([str(bright), str(_red_fg), str(_black_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def red_bg(input):
    format = ';'.join([str(bright), str(_white_fg), str(_red_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def green(input):
    format = ';'.join([str(bright), str(_green_fg), str(_black_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def green_bg(input):
    format = ';'.join([str(bright), str(_white_fg), str(_green_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def yellow(input):
    format = ';'.join([str(bright), str(_yellow_fg), str(_black_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def yellow_bg(input):
    format = ';'.join([str(bright), str(_white_fg), str(_yello_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def blue(input):
    format = ';'.join([str(bright), str(_blue_fg), str(_black_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def blue_bg(input):
    format = ';'.join([str(bright), str(_white_fg), str(_blue_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def pink(input):
    format = ';'.join([str(bright), str(_pink_fg), str(_black_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def pink_bg(input):
    format = ';'.join([str(bright), str(_white_fg), str(_pink_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def white(input):
    format = ';'.join([str(bright), str(_white_fg), str(_black_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)


def white_bg(input):
    format = ';'.join([str(bright), str(_black_fg), str(_white_bg)])
    return '\x1b[%sm%s\x1b[0m' % (format, input)
