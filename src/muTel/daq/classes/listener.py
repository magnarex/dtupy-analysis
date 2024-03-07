import threading
import serial
import time

class Listener(threading.Thread):
    def __init__(self,port,baud_rate=38400):
        self.port = port
        self.baud_rate = baud_rate
        self.connect()

        # super().__init__(group, target, name, args, kwargs, daemon=daemon)
    
    def connect(self):
        self.serial = serial.Serial(self.port,self.baud_rate)

    def read_loop(self):
        try:
            assert self.serial
        except AssertionError as err:
            print('The object has not been connected yet to a port. Waiting to connect to {port}'.format(port=self.port))
        
        while True:
            data = self.serial.read(9999)
            if len(data) > 0:
                print(int.from_bytes(data))
            time.sleep(0.5)
            # print("I'm running the loop")
        self.serial.close()
    
    def start(self):
        self.thread = threading.Thread(
            target= self.read_loop,
            daemon=True
        )
        self.thread.start()
    
    def listen(self,listen_time):
        self.start()
        time.sleep(listen_time)

if __name__ == '__main__':
    port = r"\\.\CNCA0"
    listener = Listener(port)
    listener.listen(100)


# while True:
#     data = ser.read(9999)
#     if len(data) > 0:
#         print('Got: {data}'.format(data=data.decode('utf-8')))

#     time.sleep(0.5)
#     # print('not blocked')

# ser.close()