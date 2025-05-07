import time


class Printter:

    @staticmethod
    def debug(*message):
        print('_____ %s [DEBUG]: %s' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), ",".join(message)))

    @staticmethod
    def info(*message):
        print('>>>>> %s [INFO]: %s' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), ",".join(message)))

    @staticmethod
    def warn(*message):
        print('***** %s [WARN]: %s' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), ",".join(message)))

    @staticmethod
    def error(*message):
        print('@@@@@ %s [ERROR]: %s' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), ",".join(message)))
