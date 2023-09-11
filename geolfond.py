#!venv/bin/python3
import Parser


#logging.basicConfig(format='%(levelname)s, %(asctime)s: %(message)s (LINE: (%(lineno)d) [%(filename)s]', datefmt='%I:%M:%S %p', level=logging.DEBUG)

def parse_reestr_full() -> None:
    #: Пропарсить весь реестр
    try:
        f = Parser._filter()
        for i in f.keys():
            data = Parser.client.get_data_from_reestr(filter=i)
            df = Parser.create_df(data)
            Parser.save_df(df, i)

    except Exception as e:
        raise

def run_code() -> None:
    #: Функция для скачивания только oil

    try:
        logger = Parser.config_logger('geolfond')
        parser = Parser.client()
        logger.info('Начинаю загрузку')
        data = parser.get_data_from_reestr(filter='oil')

        df = Parser.create_df(data)

        Parser.save_df(df, 'oil')

        x = Parser.sender()
        msg = x.create_message(all=True)
        x.send_message(msg)
    except: 
        logger.critical('Ошибка выгрузки')
        x = Parser.sender()
        msg = x.create_log_message()
        x.send_message(msg)

        raise

if __name__ == "__main__":
    run_code()