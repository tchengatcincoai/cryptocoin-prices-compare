from flask_script import Manager

from cryptocoincompare.application import app
from flask_script import Command, Option
from gunicorn.app.base import Application


class GunicornServer(Command):

    description = 'Run the app within Gunicorn'

    def __init__(self, host='0.0.0.0', port=8000, workers=6):

        self.port = port
        self.host = host
        self.workers = workers

    def get_options(self):
        return (
            Option('-t', '--host',
                   dest='host',
                   default=self.host),

            Option('-p', '--port',
                   dest='port',
                   type=int,
                   default=self.port),

            Option('-w', '--workers',
                   dest='workers',
                   type=int,
                   default=self.workers),
        )

    def __call__(self, *args, **kwargs):

        host = kwargs['host']
        port = kwargs['port']
        workers = kwargs['workers']

        def remove_non_gunicorn_command_line_args():
            import sys
            args_to_remove = ['--port','-p']
            def args_filter(name_or_value):
                keep = not args_to_remove.count(name_or_value)
                if keep:
                    previous = sys.argv[sys.argv.index(name_or_value) - 1]
                    keep = not args_to_remove.count(previous)
                return keep
            sys.argv = filter(args_filter, sys.argv)

        remove_non_gunicorn_command_line_args()

        class FlaskApplication(Application):
            def init(self, parser, opts, args):
                return {
                    'bind': '{0}:{1}'.format(host, port),
                    'workers': workers
                }

            def load(self):
                return app

        FlaskApplication().run()

manager = Manager(app)
manager.add_command('gunicorn', GunicornServer())

if __name__ == "__main__":
    manager.run()