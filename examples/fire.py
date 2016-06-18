from tasks import say
import time

if __name__ == '__main__':
    for i in range(1, 1000):
        time.sleep(0.01)
        print say.apply_async(('', ), queue='popcorn')
