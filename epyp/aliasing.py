import pandas as pd
import regex as re
from collections.abc import Callable # For type hints, represents functions and such


########################
### Alias class and  ###
########################

class Alias:
    
    def __init__(self, regex:str,
                 alias_str:str=None, alias_fct:Callable=None):
        self.regex = re.compile(regex)
        if alias_str is not None:
            self.alias_fct = lambda df,grps: df[alias_str.format(*grps)]
        elif alias_fct is not None:
            self.alias_fct = alias_fct
        else:
            raise ValueError("alias or alias_fct is required")
        # AliasGroups referencing current alias
        self.ref_alias_grps = []

class AliasGroup:
    
    def __init__(self, name):
        self._name = name
        self.aliases = []
        # Sources referencing current group
        self.ref_sources = []
    
    @property
    def name(self):
        return self._name    

    
#########################
### DataFrame proxies ###
#########################

class DFProxyGetItemSpy:
    
    def __init__(self, aliases, vnames_valid):
        self._aliases = aliases
        self._aliasing_map = {}
        self._vnames_used = []
        self._vnames_valid = vnames_valid
        self._df_dum = pd.DataFrame({'dum':[1]})
        self._N_call = 0
    
    # __getitem__ defined to apply alias-checking logic
    def __getitem__(self, key):
        # Cheking number of getitem call to prevent
        #  infinite loop, and raise error
        self._N_call += 1
        if self._N_call > 1000:
            raise ValueError(f"Infinite loop in aliasing spy: {key}")
        # If key already in _aliasing_map, nothing to check
        if key in self._aliasing_map:
            return self._df_dum['dum']
        # Otherwise, checking if key matches an alias regex
        #  and if so, adding to _aliasing_map
        for a in self._aliases:
            match = re.search(a.regex, key)
            if match:
                grps = match.groups()
                fct = a.alias_fct
                self._aliasing_map[key] = (fct, grps)
                # Applying function using the same getitem
                #  to chain alias detection
                return fct(self, grps)
        # If no match is obtained, end of aliasing chain and
        #  None registered in _aliasing_map for current key
        #  and var added to _vnames_used (list required valid vars)
        if key not in self._vnames_valid:
            raise KeyError(f"\'{key}\' is not a valid variable name")
        self._aliasing_map[key] = None
        self._vnames_used.append(key)
        # Returning a dummy series to limit the risk of error raising
        #  if a DataFrame-specific attribute/method is used
        return self._df_dum['dum']
    
    # __getattr__ defined to pass any attribute to the dummy DataFrame
    def __getattr__(self, name):
        return self._df_dum.__getattribute__(name)
    
    # Reset functions to reuse the same spy object
    def reset(self):
        self._N_call = 0
        self._aliasing_map = {}
        self._vnames_used = []
    def reset_vnames_used(self):
        self._vnames_used = []
    
    @property
    def aliasing_map(self):
        return self._aliasing_map
    
    @property
    def vnames_used(self):
        return self._vnames_used
    
    
class DFProxyAliasing:
    
    def __init__(self, dataframe, aliasing_map):
        self._aliasing_map = aliasing_map
        self._df = dataframe
    
    # __getitem__ defined to handle aliases thanks to the mapping
    #  previously built using DFProxyGetItemSpy object
    def __getitem__(self, key):
        # If key corresponds to final var, standard df __getitem__ used
        aliasing = self._aliasing_map[key]
        if aliasing is None:
            return self._df[key]
        # Otherwise, apply function described by the map
        (fct, grps) = aliasing
        return fct(self, grps)
    
    def __getattr__(self, name):
        return self._df.__getattribute__(name)