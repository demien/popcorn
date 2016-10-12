A = {'name': 'demien'}

def foo():
    # A.clear()
    # A['age'] = 1
    global A
    A = {}

foo()
print A
