import os
import logging
import regex as re
import pandas as pd
from dataclasses import dataclass # Simplifies basic classes creation

from datasource import *
from aliasing import *


class Manager:
    
    # Class constant
    VALID_FILE_TYPES = ('h5')
    
    def __init__(self):
        self._sources = {}
        self._alias_grps = {}
        self._aliases = []
        self._preload_data = False
    
    ###################################
    ### Manager flags getter/setter ###
    ###################################
    
    @property
    def preload_data(self):
        return self._preload_data
    
    @preload_data.setter
    def preload_data(self, b):
        self._preload_data = b
        for src in self._sources:
            if hasattr(src, 'preload_data'):
                src.preload_data = self._preload_data
    
    #####################################
    ### Methods to handle datasources ###
    #####################################
    
    def add_source_EP_h5(self, name, filepath, alias_grp_name=None):
        src = DataSourceEPH5(name, filepath, self.preload_data)
        self._sources[name] = src
        src.load()
        if alias_grp_name is not None:
            try:
                ag = self._alias_grps[alias_grp_name]
                self.set_alias_grp(alias_grp_name, assigned_srcs=[name])
            except KeyError:
                self.add_alias_grp(alias_grp_name, assigned_srcs=[name])           
        
    def del_source(self, name):
        try:
            src = self._sources[name]
        except KeyError:
            logging.warning(f"Cannot remove \'{name}\': it does not exist")
            return
        # Deleting reference to source in alias_grps
        for ag in src.ref_alias_grps:
            del ag.ref_sources[src]
        # Deleting object from manager
        del src

    
    ######################################
    ### Methods to handle alias groups ###
    ######################################
    
    def set_alias_grp(self, name, assigned_srcs=[]):
        try:
            ag = self._alias_grps[name]
        except KeyError:
            logging.warning(f"Cannot set \'{name}\': it does not exist")
            return
        for src_name in assigned_srcs:
            try:
                src = self._sources[src_name]
                # Reference to alias_grp in source
                src.alias_grp = ag
                # Reference to source in alias_grp
                ag.ref_sources.append(src)
            except KeyError:
                logging.warning(f"Cannot assign \'{name}\' to"+
                                f"\'{src_name}\': the latter does not exist")
                
    def del_alias_grp(self, name, logKeyError=True):
        try:
            ag = self._alias_grps[name]
        except KeyError:
            if logKeyError:
                logging.warning(f"Cannot remove \'{name}\':"+
                                " it does not exist")
            return
        # Deleting reference to alias_grp in sources
        for src in ag.ref_sources:
            src.alias_grp = None
        # Deleting reference to alias_grp in aliases
        for a in alias_grp.aliases:
            a.ref_alias_grps.remove(ag)
        # Deleting object from manager
        del ag
            
    def add_alias_grp(self, name, assigned_srcs=[]):
        # If name was already used, deleting previous instance
        self.del_alias_grp(name, logKeyError=False)
        # Creating new alias_grp and assigning to sources
        ag = AliasGroup(name)
        self._alias_grps[name] = ag
        self.set_alias_grp(name, assigned_srcs)
            
    #################################
    ### Methods to handle aliases ###
    #################################
    
    def add_alias(self, 
                  regex:str,
                  alias_str:str=None,
                  alias_fct:Callable=None,
                  assigned_alias_grps:list=[]
                 ):
        a = Alias(regex, alias_str, alias_fct)
        self._aliases.append(a)
        if not isinstance(assigned_alias_grps,list):
            assigned_alias_grps = [assigned_alias_grps]
        for ag_name in assigned_alias_grps:
            try:
                ag = self._alias_grps[ag_name]
                ag.aliases.append(a)
                a.ref_alias_grps.append(ag)
            except KeyError:
                logging.warning(f"Cannot assign alias to"+
                                f"\'{ag_name}\': the latter does not exist")
                
    def del_alias(self, i:int):
        a = self._aliases[i]
        for ag in a.ref_alias_grps:
            ag.aliases.remove(a)
        del self._aliases[i]
    
    ###############################
    ### Methods to extract data ###
    ###############################
            
    def get_df(self, vnames=None, sources=None, loc=None, iloc=None):
        # If no source is given, all existing sources are outputed
        sources = sources or self._sources.keys()
        df_dict = {}
        for src_name in sources:
            # Reading source
            try:
                src = self._sources[src_name]
            except KeyError:
                logging.warning(f"Source \'{src_name}\': does not exist")
                break
            df_dict[src_name] = getAliasedDF(src, vnames, loc, iloc)
        return df_dict  
    
    
def getAliasedDF(source, vnames=None, loc=None, iloc=None):
    # Checking if aliases are defined for the given source
    ag = source.alias_grp
    alias_flag = not ((ag is None) or (not bool(ag.aliases)))
    aliases = [] if ag is None else ag.aliases
    # Creating a dataframe proxy object to detect var
    #  names used in the loc filter if any
    vnames_valid = source.vardict.keys()
    spy = DFProxyGetItemSpy(aliases, vnames_valid)
    vnames_loc = []
    if loc is not None:
        _ = loc(spy)
        vnames_loc = spy.vnames_used
    # Handling cases without aliases
    if (vnames is None) or (not alias_flag) :
        # If no vnames given, dict keys are used -> no alias
        vnames = vnames or vnames_valid
        if loc is None:
            df = source.get_df(vnames)
            if iloc is not None:
                df = df.iloc[iloc]
            return df
        else:
            df = src.get_df(vnames + col_list_loc)
            if iloc is not None:
                df = df.iloc[iloc]
            df_dict[srck] = df.loc[loc(df)][vnames]
            return df
    # Generic case with aliases
    for vname in vnames:
        _ = spy[vname]
    aliasing_map = spy.aliasing_map
    vnames_used = list(set(vnames_loc + spy.vnames_used))
    df_used = source.get_df(vnames_used)
    df_proxy = DFProxyAliasing(df_used, aliasing_map)
    series_out = []
    for vname in vnames:
        out = df_proxy[vname]
        if isinstance(out, list):
            series_out.extend(out)
        else:
            series_out.append(out.rename(vname))
    df = pd.concat(series_out, axis=1)
    if loc is not None:
        df = df.loc[loc(df_proxy)]
    if iloc is not None:
        df = df.iloc[iloc]
    return df