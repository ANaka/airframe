from .airtable import Airtable
import pandas as pd
import os
import numpy as np
import boto3
import traceback
import tempfile
import datetime

global context_table
context_table = None

class PandasAirtable(Airtable):
    '''Extends Airtable class from python-airtable-wrapper.
    Automatically authenticates API access.
    Includes methods to download tables to pandas DataFrame and
    to upload attachments to Airtable via S3.

    For details on parent: https://github.com/gtalarico/airtable-python-wrapper
    '''
    
    def __init__(self, primary_key=None, *args, **kwargs):
        
        self._primary_key = primary_key
        self._df = None
        super().__init__(*args, **kwargs)
    
    @property
    def df(self):
        if self._df is None:
            self._df = self.to_df()
        return self._df
    
    @df.setter
    def df(self, df):
        self._df = df
    
    @property
    def primary_key(self):
        if self._primary_key is None:
            self._primary_key = self.df.columns[0]
        return self._primary_key
    
    def to_df(self):
        '''Returns pandas DataFrame of current table. Note that this is slow-
        takes several seconds for a table with 1000 records.
        '''
        records = self.get_all()
        df = airtable_records_to_DataFrame(records)
        return df

    
    def get_all_flat(self):
        '''Same function as .get_all() but returns a list of flat dictionaries;
        i.e. the Airtable id is merged with the fields dictionary
        '''
        flatrecs = list(self.to_df().reset_index().to_dict(orient='index').values())
        return flatrecs

    def create_s3_client(self):
        '''Creates an s3 client. Relies on configured AWS cli i.e. credentials
        must be present at '~/.aws/credentials'

        Might be wise to remove this to make this class more readily usable by
        people without AWS capabilities
        '''
        self.s3 = boto3.client('s3', region_name='us-west-1')
                            
    def get_record_id(self, field_name, field_value):
        recs = self.search(field_name=field_name, field_value=field_value)
        if len(recs) == 1:
            return recs[0]['id']
        elif len(recs) == 0:
            print('No matching records')
        elif len(recs) > 1:
            print('More than one matching record')
    
    def get(self, record_id, as_series=True):
        if as_series:
            series = airtable_record_to_Series(Airtable.get(self, record_id=record_id))
            series.airtable.table = self
            return series
        else:
            return Airtable.get(self,record_id=record_id)
        
    
    def get_many(self, record_ids, as_df=True):
        records = [Airtable.get(self, record_id=record_id) for record_id in record_ids]
        if as_df:
            df = airtable_records_to_DataFrame(records)
            df.airtable.table = self
            return df
        else:
            return records
    
    def __enter__(self):
        global context_table
        context_table = self
        return self
    
    def __exit__(self, exc_type, exc_value, tb):
        global context_table
        context_table = None
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            # return False # uncomment to pass exception through

        return True
    
        
    def upsert(self, record_id, fields, typecast=True):
        try:
            assert record_id is not None
            return self.update(record_id=record_id, fields=fields, typecast=typecast)
        except:
            return self.insert(fields=fields, typecast=typecast)

    def robust_upsert(self, record_id, fields, typecast=True):
        try:
            assert record_id is not None
            return self.robust_update(record_id=record_id, fields=fields, typecast=typecast)
        except:
            return self.robust_insert(fields=fields, typecast=typecast)

    def robust_insert(self, fields, typecast=True):
        try:
            fields = {k:typecast_airtable_value(v) for k,v in fields.items()}
            return self.insert(fields, typecast=typecast)
        except:
            return self.insert_one_field_at_a_time(fields=fields, typecast=typecast)
        
    def robust_update(self, record_id, fields,typecast=True):
        
        fields = {k:typecast_airtable_value(v) for k,v in fields.items()}
        
        try:
            return self.update(
                record_id=record_id,
                fields=fields,
                typecast=True,
            )
        except:
            return self.update_one_field_at_a_time(record_id, fields, typecast=typecast)
                        
    def insert_one_field_at_a_time(self, fields, typecast=True):
        record = None
        for key,val in fields.items():
            try:
                record = self.insert(fields={key:val}, typecast=typecast)
            except:
                pass
            if record is not None:
                break
        record_id = record['id']
        return self.update_one_field_at_a_time(record_id, fields, typecast=typecast)
        
    def update_one_field_at_a_time(self, record_id, fields, typecast=True):
        outputs = {'fails':[]}
        for key,val in fields.items():
            try:
                field = {key: val}
                record = self.update(
                            record_id=record_id,
                            fields=field,
                            typecast=typecast,
                        )
                outputs.update(record)
            except:
                outputs['fails'].append({'fields':field})
        return outputs
    
    def upload_attachment_to_airtable_via_s3(
        self,
        filepath,
        s3_bucket,
        s3_key,
        record_id,
        field_name,
        s3_url_lifetime=300,
        delete_local_file_when_done=False,
        keep_old_attachments=True,
        s3_client=None,
        ):
        ''' Uploads a file to an attachment field for a single Airtable record.
        Works by uploading file first to S3, creating a short-lived public URL
        pointing to the file in S3, and then sending that URL to Airtable.

        

        Arguments:
        filepath -- local path of file to upload. String or Path object
        s3_bucket -- S3 bucket where file will be uploaded. String
        s3_key -- S3 key where file will be uploaded. String
        record_id -- Airtable record to update e.g. 'rec010N7Tt4tWxXTm'. String
        field_name -- Airtable attachment field for upload. String
        s3_url_lifetime -- Lifetime of public S3 url in seconds. Int
        delete_local_file_when_done -- if True, deletes local file after upload
        delete_s3_file_when_done -- if True, deletes S3 copy after upload. BROKEN
        '''

        if s3_client is None:
            s3_client = create_s3_client()

        s3_client.upload_file(
            filepath,
            s3_bucket,
            s3_key)

        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': s3_bucket,
                'Key': s3_key
            },
            ExpiresIn=s3_url_lifetime
        )
        
        attachments = None
        if keep_old_attachments:
            rec = self.get(record_id=record_id, as_series=False)['fields']
            attachments = rec.get(field_name) # returns None if nothing is in this field
        if attachments is None: 
            attachments = []
        attachments.append({'url': url})
        fields = {field_name: attachments}  # needs to be dict inside list
        self.update(
            record_id=record_id,
            fields=fields
        )

        if delete_local_file_when_done:
            os.remove(filepath)

       
    
                            
@pd.api.extensions.register_series_accessor('airtable')
class AirRow:
    
    def __init__(
        self,
        series,
        table:PandasAirtable=None,
        record_id:str=None,
        primary_key:str=None,
    ):
        self.series = series
        self._table = table
        self._record_id = record_id
        self._primary_key = primary_key

    def __repr__(self):
        s = self.series.__repr__()
        return f'AirRow ({self.table_name}) \n{s}'
    
    @property
    def table(self):
        if context_table is not None:
            return context_table
        else:
            return self._table
    
    @table.setter
    def table(self, table):
        self._table = table
     
    @property
    def fields(self):
        return self.series.convert_dtypes().to_dict()
    
    @property
    def table_name(self):
        try:
            return self.table.table_name
        except:
            return 'None'
    
    @property
    def record_id(self):
        '''If you manually set the record_id attribute, it will keep that one
        otherwise it will use the name of the series if it looks like a record_id
        lastly, it will query Airtable on its primary key
        '''
        if self._record_id is None:
            if self._check_if_name_is_rec_id():
                return self.series.name
            else:
                return self.get_record_id(field_name=self.primary_key)
        else:
            return self._record_id
    
    @record_id.setter
    def record_id(self, record_id):
        self._record_id = record_id
    
    def _check_if_name_is_rec_id(self):
        name = self.series.name
        if name is None:
            return False
        try:
            if len(name) != 17:  # airtable record_id length
                return False
        except TypeError:
            return False
        if name[:3] == 'rec':  # starts with rec
            return True
        else:
            return False
        
    def get_record_id(self, field_name, table=None):
        if table is None:
            table = self.table
        return table.get_record_id(field_name=field_name, field_value=self.series[field_name])
    
    @property
    def primary_key(self):
        '''If you manually set a primary key for this instance, it will keep using that to track its record_id
        otherwise, it will look at the primary key of the associated PandasAirtable and use that
        '''
        if self._primary_key is None:
            self._primary_key = self.table.primary_key
        return self._primary_key
    
    @primary_key.setter
    def primary_key(self, primary_key):
        self._primary_key = primary_key
    
    def insert(
        self,
            field_names=None,
            airtable=None,
            robust=True,
            typecast=True,
            ):
        if airtable is None:
            airtable = self.table
            
        assert airtable is not None
        
        fields = self.fields
        if field_names is not None:
            fields = {k:v for k,v in fields.items() if k in field_names}
        if robust:
             record = airtable.robust_insert(fields, typecast=typecast)
        else:
            record = airtable.robust_insert(fields, typecast=typecast)
        self.record_id = record['id']
        return record
        
    def update(
            self,
            field_names=None,
            airtable=None,
            robust=True,
            typecast=True,
            ):
        if airtable is None:
            airtable = self.table
        assert airtable is not None
        
        fields = self.fields
        if field_names is not None:
            fields = {k:v for k,v in fields.items() if k in field_names}
        if robust:
            record = airtable.robust_update(record_id=self.record_id, fields=fields,typecast=typecast)
        else:
            record = airtable.update(record_id=self.record_id, fields=fields, typecast=typecast)
        try:
            self.record_id = record['id']
            return record
        except:
            print('Update failed')
        
    
    def delete(
            self,
            airtable=None,
            ):
        if airtable is None:
            airtable = self.table
        assert airtable is not None
        
        return airtable.delete(record_id=self.record_id)
        
    def upsert(
            self,
            field_names=None,
            airtable=None,
            robust=True,
            typecast=True
            ):
        if airtable is None:
            airtable = self.table
        assert airtable is not None
        
        fields = self.fields
        if field_names is not None:
            fields = {k:v for k,v in fields.items() if k in field_names}
        
        try:
            record_id = self.record_id
        except:
            record_id = None
            
        if robust:
            record = airtable.robust_upsert(record_id=record_id, fields=fields, typecast=typecast)
        else:
            record = airtable.upsert(record_id=record_id, fields=fields, typecast=typecast)
        return record
    
    
@pd.api.extensions.register_dataframe_accessor('airtable')
class AirDataFrame:
    
    def __init__(
        self,
        df,
        table:PandasAirtable=None,
        primary_key:str=None,
    ):
        self._df = df
        self._table = table
        self._primary_key = primary_key
    
    @property
    def df(self):
        if self._df is None:
            self.get()
        return self._df
    
    @df.setter
    def df(self, df):
        self._df = df
    
    @property
    def table_name(self):
        try:
            return self.table.table_name
        except:
            return 'None'
    
    @property
    def table(self):
        if context_table is not None:
            return context_table
        else:
            return self._table
    
    @table.setter
    def table(self, table):
        self._table = table
    
    @property
    def primary_key(self):
        '''By default - check associated table first, then use first column
        (bc DataFrame columns are more likely to be rearranged)
        '''
        if self._primary_key is None:
            try:
                self._primary_key = self.table.primary_key
            except:
                self._primary_key = self.df.columns[0]
        return self._primary_key
    
    @primary_key.setter
    def primary_key(self, primary_key):
        self._primary_key = primary_key
    
    def look(self):
        'Pulls data from Airtable but does not change the parent DataFrame'
        return self.table.to_df()
    
    def get(self):
        'Pulls data from Airtable and reconstructs parent DataFrame'
        self._df = self.table.to_df()
        return self._reconstruct()
    
    def get_row(self, index):
        row = self.df.iloc[index]
        row.airtable.table = self.table
        row.airtable.primary_key = self.primary_key
        return row
 
    def _reconstruct(self):
        _df = pd.DataFrame(self._df)
        _df.airtable.table = self.table
        _df.airtable.primary_key = self.primary_key
        return _df
    
    def _prep_df(self,
                primary_key=None,
                airtable=None,
                index=None,
                columns=None, 
                ):
        if airtable is None:
            airtable = self.table
        assert airtable is not None
        
        if primary_key is None:
            primary_key = self.primary_key
        
        df = self.df
        if columns is not None:
            df = df.loc[:, columns]
        if index is not None:
            df = df.loc[index, :]
        return airtable, primary_key, df
    
    def update(self,
               primary_key=None,
               airtable=None,
               index=None,
               columns=None,
               typecast=True,
               robust=True,
               ):
        
        airtable, primary_key, df = self._prep_df(primary_key=primary_key,
            airtable=airtable, index=index, columns=columns)
        
        records = []
        for row_index, row in df.iterrows():
            row.airtable.table = airtable
            if df.index.name == 'record_id':
                row.airtable.record_id = row_index
            else:
                row.airtable.primary_key = primary_key
            _rec = row.airtable.update(typecast=typecast, robust=robust)
            records.append(_rec)
        return records
            
    def insert(self,
               primary_key=None,
               airtable=None,
               index=None,
               columns=None,
               typecast=True,
               robust=True,
               ):
        
        airtable, primary_key, df = self._prep_df(primary_key=primary_key,
            airtable=airtable, index=index, columns=columns)
        
        records = []
        for row_index, row in df.iterrows():
            row.airtable.table = airtable
            row.airtable.primary_key = primary_key
            _rec = row.airtable.insert(typecast=typecast, robust=robust)
            records.append(_rec)
        return records
    
    def upsert(self,
               primary_key=None,
               airtable=None,
               index=None,
               columns=None,
               typecast=True,
               robust=True,
               ):
        
        airtable, primary_key, df = self._prep_df(primary_key=primary_key,
            airtable=airtable, index=index, columns=columns)
        
        records = []
        for row_index, row in df.iterrows():
            row.airtable.table = airtable
            if df.index.name == 'record_id':
                row.airtable.record_id = row_index
            else:
                row.airtable.primary_key = primary_key
            _rec = row.airtable.upsert(typecast=typecast, robust=robust)
            records.append(_rec)
        return records
            
    def delete(self,
               primary_key=None,
               airtable=None,
               index=None,
               columns=None
               ):
        
        airtable, primary_key, df = self._prep_df(primary_key=primary_key,
            airtable=airtable, index=index, columns=columns)
        
        records = []
        for row_index, row in df.iterrows():
            row.airtable.table = airtable
            if df.index.name == 'record_id':
                row.airtable.record_id = row_index
            else:
                row.airtable.primary_key = primary_key
            _rec = row.airtable.delete()
            records.append(_rec)
        return records
    
def airtable_record_to_Series(record):
    return pd.Series(record['fields'], name=record['id'])
    
def airtable_records_to_DataFrame(records):
    df = pd.DataFrame.from_records((r['fields'] for r in records), index=[
            record['id'] for record in records])
    df.index.name = 'record_id'
    return df
    
def create_s3_client(region_name='us-west-1'):
    '''Creates an s3 client. Relies on configured AWS cli i.e. credentials
    must be present at '~/.aws/credentials'

    Might be wise to remove this to make this class more readily usable by
    people without AWS capabilities
    '''
    return boto3.client('s3', region_name=region_name)    

def upload_attachment_to_airtable_via_s3(
        airtable,
        filepath,
        s3_key,
        record_id,
        field_name,
        s3_bucket=None,
        s3_url_lifetime=300,
        delete_local_file_when_done=False,
        delete_s3_file_when_done=False,
        keep_old_attachments=True,
        s3_client=None,
        ):
        ''' Uploads a file to an attachment field for a single Airtable record.
        Works by uploading file first to S3, creating a short-lived public URL
        pointing to the file in S3, and then sending that URL to Airtable.

        There's a potential security hazard here - the public URL is accessible
        to anyone who knows where to look for it during its short lifetime. It
        might be possible to restrict access to certain IPs if we can figure out
        what Airtable's IP address is. On the to-do list.

        Arguments:
        filepath -- local path of file to upload. String or Path object
        s3_bucket -- S3 bucket where file will be uploaded. String
        s3_key -- S3 key where file will be uploaded. String
        record_id -- Airtable record to update e.g. 'rec010N7Tt4tWxXTm'. String
        field_name -- Airtable attachment field for upload. String
        s3_url_lifetime -- Lifetime of public S3 url in seconds. Int
        delete_local_file_when_done -- if True, deletes local file after upload
        delete_s3_file_when_done -- if True, deletes S3 copy after upload. BROKEN
        '''

        if s3_client is None:
            s3_client = create_s3_client()

        if not s3_bucket:
            s3_bucket = os.environ['TEMP_FILES_BUCKET']

        s3_client.upload_file(
            filepath,
            s3_bucket,
            s3_key)

        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': s3_bucket,
                'Key': s3_key
            },
            ExpiresIn=s3_url_lifetime
        )
        
        attachments = None
        if keep_old_attachments:
            rec = airtable.get(record_id=record_id, as_series=False)['fields']
            attachments = rec.get(field_name) # returns None if nothing is in this field
        if attachments == None: 
            attachments = []
        attachments.append({'url': url})
        fields = {field_name: attachments}  # needs to be dict inside list
        airtable.update(
            record_id=record_id,
            fields=fields
        )

        if delete_local_file_when_done:
            os.remove(filepath)

        if delete_s3_file_when_done:
            # turning this on stops things from getting into airtable -
            # they appear quickly then are deleted. maybe it is too fast?
            s3_client.delete_object(
                Bucket=s3_bucket,
                Key=s3_key
            )

def typecast_airtable_value(value):
    if isinstance(value, list):
        return value
    elif isinstance(value, np.int64):
        return int(value)
    # elif value is None:
    #     return ''
    elif pd.isna(value):
        return None
    else:
        return value


                 
def upload_df_to_airtable(
        airtable,
        df,
        primary_key='id',
        overwrite=False,
        try_one_field_at_a_time=False
        ):
    """upload pandas DataFrame to airtable

    Args:
        df (pandas DataFrame): should match format of airtable
        primary_key (str): primary key on airtable (first column). Defaults to 'id'.
        overwrite (bool): if yes, this runs an upsert. Defaults to False.
        try_one_field_at_a_time (bool, optional): can sometimes solve problems. Defaults to False.
    """        
    for i, row in df.iterrows():
        upload_Series_to_airtable(airtable, row, primary_key, overwrite, try_one_field_at_a_time)



def upload_Series_to_airtable(
        airtable, 
        data: pd.Series,
        primary_key='id',
        overwrite=False,
        try_one_field_at_a_time=False,
        ):
    """upload pandas series to airtable

    Args:
        df_row (row of pandas Dataframe i.e. a Series): 
        primary_key (str): primary key on airtable (first column). Defaults to 'id'.
        overwrite (bool): if yes, this runs an upsert. Defaults to False.
        try_one_field_at_a_time (bool, optional): can sometimes solve problems. Defaults to False.
    """        
    
    fields = data.to_dict()
    clean_fields = {}
    for key, value in fields.items():  # airtable doesnt like numpy ints
            clean_fields[key] = typecast_airtable_value(value)   

    fields = clean_fields
    matching_recs = airtable.search(
    field_name=primary_key, field_value=fields[primary_key])
    failed_to_upload = False
    if matching_recs:
        if len(matching_recs) > 1:
            print('WARNING: POSSIBLE DUPLICATE RECORDS IN AIRTABLE')
        if not overwrite:
            print('''
            Warning: this experiment is already present in self
            Set overwrite=True if you want to overwrite
            ''')
        elif overwrite:
            try:
                record_id = matching_recs[0]['id']
                airtable.update(
                    record_id=record_id,
                    fields=fields,
                    typecast=True,
                )
            except:
                failed_to_upload = True
                print('Upload to Airtable failed')
        if failed_to_upload & try_one_field_at_a_time:
            print('Trying to upload one field at a time')
            for key, value in fields.items():
                try:
                    record_id = matching_recs[0]['id']
                    airtable.update(
                        record_id=record_id,
                        fields={key: value},
                        typecast=True,
                    )
                except:
                    print('Unable to upload ' + key)
    

    elif not matching_recs:
        try:
            airtable.insert(
                fields=fields,
                typecast=True,
            )

        except:
            print('Upload to Airtable failed')
            if try_one_field_at_a_time:
                print('Trying to upload one field at a time')
                airtable.insert(
                    fields={primary_key:fields[primary_key]},
                    typecast=True,
                )
                matching_rec = airtable.match(
                    field_name=primary_key, field_value=fields[primary_key])
                record_id = matching_rec['id']
                for key, value in fields.items():
                    try:
                        airtable.update(
                            record_id=record_id,
                            fields={key: value},
                            typecast=True,
                        )
                    except:
                        print('Unable to upload ' + key)
    
    
    

def unpack_list_field(x, delimiter=','):
    if len(x) == 1:
        try:
            return x[0]
        except:
            return x
    else:
        try:
            return delimiter.join(x)
        except:
            return x
        
        
        
class AirtableAttachment(object):
    
    def __init__(
        self,
        filepath=None,
        record: AirRow = None,
        field_name=None,
        **kwargs
    ):
        self._filepath = filepath
        self._field_name = field_name
        self._record = record
        
    @property
    def filepath(self):
        return self._filepath
    
    @property
    def record(self):
        return self._record
    
    @property
    def record_id(self):
        return self.record.record_id
    
    def upload_to_airtable(
        self,
        filepath=None,
        field_name=None,
        airtable=None,
        record_id=None,
        s3_key=None,
        s3_bucket=None,
        keep_old_attachments=True,
    ):
        if airtable is None:
            airtable = self.record.table
            
        if filepath is None:
            filepath = self.filepath
            
        if record_id is None:
            record_id = self.record_id
        
        if field_name is None:
            field_name = self._field_name 
            
        if s3_key is None:
            now = datetime.datetime.now()
            nowstr = now.isoformat().replace(':','_').replace('.','_')
            s3_key = f'temp_imgs/{nowstr}.png'
            
       
        airtable.upload_attachment_to_airtable_via_s3(
            filepath=filepath,
            s3_key=s3_key,
            record_id=record_id,
            s3_bucket=s3_bucket,
            field_name=field_name,
            keep_old_attachments=keep_old_attachments,
        )
    
    @classmethod
    def from_matplotlib_figure(
        cls, 
        figure,
        format='png',
        dpi=300,
        bbox_inches='tight',
        savefig_kwargs=None,
        **kwargs):
        if savefig_kwargs is None:
            savefig_kwargs = {}
        with tempfile.NamedTemporaryFile() as tmp:
            figure.savefig(tmp, format=format, dpi=dpi, bbox_inches=bbox_inches, **savefig_kwargs)
            return cls(filepath=tmp.name **kwargs)


class AuthenticatedPandasAirtable(PandasAirtable):
    
    def __init__(
            self,
            table_name,
            base_key=None,
            api_key=None,
            region_name='us-west-1',
            *args,
            **kwargs,
            ):
        '''Authenticating an instance of Airtable() requires an API key to airtable
        as well as a key identifying the base. If these are not passed as arguments to
        the codaAirtable() constructor, it will attempt to retrieve them from AWS.
        Successful retrieval only works if you have AWS CLI configured on your machine
        using an account with the correct AWS permissons.
        '''
        self._cred = None
        if (base_key is None) or (api_key is None):
            self._cred = # function to get secrets goes here
            self._cred.get_defaults()
            
        if base_key is None:
            base_key = os.environ[self._cred.base_key_name]

        if api_key is None:
            api_key = os.environ[self._cred.api_key_name]
            
        super().__init__(table_name=table_name, base_key=base_key, api_key=api_key, *args, **kwargs)