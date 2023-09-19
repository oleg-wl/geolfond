#!venv/bin/python

import Parser
import datetime

def main():
    
    n = datetime.datetime.strftime(datetime.datetime.now(), '%d.%m.%Y - %H:%M')

    
    c = Parser.client()
    data = c.get_abdt_index()
    m = Parser.transformer(data=data)
    m.create_abdt_index()
        
    s = m.kdemp()

    ms = Parser.sender()
    ms.create_message(filename='not', htmlstr=s)
    ms.send_message_f()

if __name__ == "__main__":
    main()