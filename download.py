from sqlalchemy import BigInteger, String, Column, MetaData, Table
from sqlalchemy import func
from sqlalchemy import select
import os

from run_config import engine, permitted_file_names
import file_transfer
import settings

metadata = MetaData()
table = Table('pmi_sprint_download',
              metadata,
              Column('sender_name', String(200), nullable=False),
              Column('sent_time', BigInteger, nullable=False),
              Column('file_handle', String(1000), nullable=False),
              Column('file_name', String(500), nullable=False),
              Column('file_size', BigInteger, nullable=False),
              Column('url', String(1000), nullable=False),
              Column('message', String(1000), nullable=True))


def update_table():
    """
    Log newly submitted files
    :return:
    """
    metadata.create_all(engine)
    payload = file_transfer.inbox()
    for uid, package in payload['packages'].items():
        sender_name = package['sender_name']
        sent_time = int(package['sent_time'])
        for package_file in package['package_files']:
            file_handle = package_file['file_handle']
            query = table.select().where(table.c.file_handle == file_handle)
            results = engine.execute(query).fetchall()
            if len(results) == 0:
                file_name = file_handle.split('/')[-1]
                file_size = int(package_file['file_size'])
                url = package_file['url']
                message = package['mail_body'].strip() if 'mail_body' in package else None
                engine.execute(table.insert(),
                               sender_name=sender_name,
                               sent_time=sent_time,
                               file_handle=file_handle,
                               file_name=file_name,
                               file_size=file_size,
                               url=url,
                               message=message)


def download_latest():
    """
    Download and save the latest submission files possibly overwriting existing, presumably older files
    :return:
    """
    q1 = select([table.c.file_name, func.max(table.c.sent_time)]).group_by('file_name')
    latest_files = engine.execute(q1).fetchall()
    permitted = list(permitted_file_names())
    for f in latest_files:
        selection = [table.c.sender_name, table.c.file_name, table.c.url]
        q2 = select(selection).where(table.c.file_name == f['file_name'] and table.c.sent_time == f['sent_time'])
        r = engine.execute(q2).fetchone()
        file_name = r['file_name'].lower()
        sender_name = r['sender_name']
        if file_name not in permitted:
            print 'Notify %(sender_name)s that "%(file_name)s" is not a valid file name' % locals()
        # download either way just in case
        dest = os.path.join(settings.csv_dir, file_name)
        file_transfer.download(r['url'], dest)


if __name__ == '__main__':
    update_table()
    download_latest()
