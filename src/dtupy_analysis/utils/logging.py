import sys
import logging
import datetime

def begin_log(parentdir,filename):
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%d')
    filepath=f'{parentdir}/tmp/{filename}_{timestamp}.log'
    formatter = logging.Formatter('[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(filename=filepath, mode='a+')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)

    # The handlers have to be at a root level since they are the final output
    logging.basicConfig(
        level=logging.DEBUG, 
        format='[{%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=[
            file_handler,
            stream_handler
        ]
    )