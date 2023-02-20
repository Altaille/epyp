import os
import hashlib
import pandas as pd
import h5py
import re
import logging
import numpy as np


class _DataSource:
    
    def __init__(self, name, filepath, alias_grp=None):
        # Base attributes
        self.name = name
        self.filepath = filepath
        self._src_valid = None
        self._checksum = None
        
        # Data containers
        self._vardict = {}
        self._df = None
        self._df_dim = None
        
        # References
        self.alias_grp = alias_grp
        
    @property
    def vardict(self):
        return self._vardict
        
    def _clear_data(self):
        self._vardict = {}
        self._df = None
        
    def load(self):
        valid, updated = self._file_update_check()
        # Invalid file -> nothing to load, wipe previous data
        if not valid:
            logging.info(self.str_src() +
                         " -> file unvalid: clearing previous data if any")
            self._src_valid = False
            self._clear_data()
            return
        # File not modified since previous loading -> nothing to do
        if not updated:
            logging.info(self.str_src() +
                         " -> file unchanged: data not updated")
            return
        # File is new/has been updated -> loading operations
        logging.info(self.str_src() +
                     " -> new file/updated file: loading data")
        self._load_data()
        return
        
    def _file_update_check(self):
        # If file exists, hash saved to check for change between calls
        try:
            with open(self.filepath, 'rb') as f:
                hash_md5 = hashlib.md5()
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_md5.update(chunk)
                checksum = hash_md5.hexdigest()
            if checksum == self._checksum:
                updated = False
            else:
                self._checksum = checksum
                updated = True
            valid = True
            return valid, updated
        # Otherwise, warning sent to user
        except FileNotFoundError:
            logging.warning(self.str_src() +
                            " -> file does not exist")
            self._checksum = None
            valid = False
            updated = False
            return valid, updated
    
    def _clean_var_list(self, var_lst):
        list_existing = list(self._vardict.keys())
        clean_list = []
        unk_list = []
        for v in var_lst:
            # Detecting wrong var name
            if v not in list_existing:
                unk_list.append(v)
                logging.warning(self.str_src() +
                                f" -> variable \'{v}\' unknown")
            # Avoiding multiple occurences
            elif v not in clean_list:
                clean_list.append(v)
        return clean_list, unk_list
        
    def get_df(self, var_lst=None):
        # Column-wise filter if var_lst != None
        if var_lst is not None:
            var_lst_c, _ = self._clean_var_list(var_lst)
            df_sliced = self._df[var_lst_c]
        # Replace indexes by actual strings/enums
        df_sliced = self._substitute_str_enum(df_sliced)
        return df_sliced.copy()
    
    def str_src(self):
        return f"Source:\'{self.name}\' (path:\'{self.filepath}\')"
    

class DataSourceEPH5(_DataSource):
    
    def __init__(self, name, filepath, alias_grp=None, preload_data=False):
        super().__init__(name, filepath)
        self.preload_data = preload_data
        # Additionnal data containers
        self._strings = {}
        self._enums = {}
        
    def _clear_data(self):
        super()._clear_data()
        self._strings = {}
        self._enums = {}
        
    def _order_var_lst(self, var_lst):
        zipped = zip([self._vardict[v]['col'] for v in var_lst], var_lst)
        return [z[1] for z in sorted(zipped, key=lambda z:z[0])]
        
    def _load_data(self):
        try:
            with h5py.File(self.filepath, 'r') as f:
                # Reading layout description in file
                strs = [s.decode("utf-8") 
                        for s in list(f['Internal/Strings'][()])]
                try:
                    enum_arr = f['Internal/Enums'][()]
                except KeyError:
                    enum_arr = []
                vars_arr = f['Internal/Symbols'][()]
                # Creating list for each var: (name, type, unit, column)
                # - Time added first (handled differently by EP)
                # - Removing alignement 0 in EP indexes (v[010] -> v[10])
                # - Index replaced by actual string for units
                reg = re.compile(r'\[[0-9]*\]')
                f0 = lambda mo: mo.group(0).lstrip('0')
                fstr = lambda v: re.sub(reg, f0, v)
                vars_layout_list = [('TIME', 'Real', 's', 13)] + \
                                    [(fstr(t[0].decode()),   # var name
                                      t[4].decode(),         # var type
                                      strs[t[13]],           # var unit
                                      t[3],                  # column
                                     ) for t in vars_arr]
                # Sorting list by column (for data slicing)
                vars_layout_list.sort(key=lambda x:x[3])
                # Filling a dict to store useful informations more clearly
                vars_layout_dict = {t[0]:{'type':t[1],
                                          'unit':t[2],
                                          'col':t[3]} 
                                    for t in vars_layout_list}
                self._vardict = vars_layout_dict
                
                # Extracting from values table dataset if preloading enabled
                ds = f['CalcData/VarValues']
                self._df_dim = ds.shape
                if self.preload_data:
                    self._df = pd.DataFrame(data=ds[:,13:],
                                            columns=vars_layout_dict.keys())

                # Storing strings: dict to use pandas df.replace function
                self._strings = {i:s for i,s in enumerate(strs)}
                
                # Storing enums: 
                # - Data type for enumerative vars is 'Enumeration e'
                #    where 'e' is enumerative index
                # - Data value for enumerative vars is an index 'i' relative
                #    to a given enumerative total possible values
                # - h5 file stores enum_arr which associates each 'i' to a
                #    string index 's' to read the string from string table
                # Dict {enum_label: {i: s}} built to use df.replace function
                enum_dict = {}
                for (e,i,s) in enum_arr:
                    enum_label = 'Enumeration ' + str(e)
                    if enum_label in enum_dict.keys():
                        enum_dict[enum_label][i] = s
                    else:
                        enum_dict[enum_label] = {i:s}
                    self._enums = enum_dict
        # Catching hdf5 file reading errors to add more context to the message
        except OSError as err:
            raise OSError(self.str_src() + f" -> {err}")
        return
    
    def _substitute_str_enum(self, df):
        # Defining data types
        ## Going through columns and listing strings/enums
        enum_vars = []        
        str_enum_vars = []
        astype_dict = {}
        for v in df.columns:
            v_type = self._vardict[v]['type']
            if v_type == 'Real':
                astype_dict[v] = 'float64'
            elif v_type == 'Integer':
                astype_dict[v] = 'int64'
            elif v_type == 'Boolean':
                astype_dict[v] = 'boolean'
            elif v_type == 'String':
                astype_dict[v] = 'category'
                str_enum_vars.append(v)
            elif v_type in self._enums.keys():
                astype_dict[v] = 'category'
                enum_vars.append(v)
                str_enum_vars.append(v)
            else:
                raise TypeError (f"Unknown var type \'{v_type}\' \
                                 for var \'{v}\'")
        ## Applying types to dataframe (relatively costly)
        #t_in = time.time()
        df = df.astype(astype_dict, copy=False)
        #print(f"Elapsed1: {time.time() - t_in:2.3f}s")
        # Replacing enum integers by string integers
        #t_in = time.time()
        de = {v: df[v].cat.rename_categories(
            self._enums[self._vardict[v]['type']])
              for v in enum_vars}
        df[enum_vars] = pd.DataFrame(de)
        # Replacing string integers by actual strings (categorical data)
        dstr = {v: df[v].cat.rename_categories(
            self._strings)
                for v in str_enum_vars}
        df[str_enum_vars] = pd.DataFrame(dstr)
        #print(f"Elapsed2: {time.time() - t_in:2.3f}s")
        return df
        
    def get_df(self, var_lst=None):
        # If preloading enabled, working directly on dataframe 
        if self.preload_data:
            return super().get_df(var_lst, loc, iloc)
        # Otherwise, loading minimal data from hdf5 file
        else:
            ## Calling load to update the file if updated since last call
            self.load()
            ## Listing columns indices before opening the file
            if var_lst is None:
                var_lst_c = self._vardict.keys()
            else:
                var_lst_c, _ = self._clean_var_list(var_lst)
                var_lst_c = self._order_var_lst(var_lst_c)
            col_lst = [self._vardict[v]['col'] for v in var_lst_c]
            ## Opening the file and extracting the slices
            with h5py.File(self.filepath, 'r') as f:
                ds = f['CalcData/VarValues']
                ### Extracting required data
                df = pd.DataFrame(data=ds[:,col_lst],
                                  columns=var_lst_c)
                ### Replace indexes by actual strings/enums
                df = self._substitute_str_enum(df)
                if var_lst is None:
                    return df
                return df[var_lst_c]