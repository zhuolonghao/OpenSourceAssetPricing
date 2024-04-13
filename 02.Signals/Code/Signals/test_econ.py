import beaapi
beakey = '0FAD9DAD-B778-42E9-BA0E-454F0DE7BC7D'

list_of_sets = beaapi.get_data_set_list(beakey)
print(list_of_sets)  # Note the last dataset is only for speeding up metadata queries

list_of_params = beaapi.get_parameter_list(beakey, 'NIPA')
print(list_of_params)

list_of_param_vals = beaapi.get_parameter_values(beakey, 'NIPA', 'TableID')
print(list_of_param_vals)

bea_tbl_M = beaapi.get_data(beakey, datasetname='NIPA', TableName='T20804', Frequency='M', Year='2020')
econ_vars = bea_tbl_M.groupby(['SeriesCode', 'LineDescription'], as_index=False)['LineNumber'].max()
bea_tbl_Q = beaapi.get_data(beakey, datasetname='NIPA', TableName='T20404', Frequency='Q', Year='2020')
econ_vars = bea_tbl_Q.groupby(['SeriesCode', 'LineDescription'], as_index=False)['LineNumber'].max()


lvl1 = ['DPCERA', 'DDURRA', 'DNDGRA', 'DSERRA']
lvl2 = ['DMOTRA','DFDHRA','DREQRA','DODGRA','DFXARA','DCLORA','DGOERA','DONGRA',
        'DHCERA','DHUTRA','DHLCRA','DTRSRA','DRCARA','DFSARA','DIFSRA','DOTSRA']
lvl3 = ['DPCERA',
'DGDSRA',
'DDURRA',
'DMOTRA',
'DNMVRA',
'DNPVRA',
'DMVPRA',
'DFDHRA',
'DFFFRA',
'DAPPRA',
'DUTERA',
'DTOORA',
'DREQRA',
'DVAPRA',
'DSPGRA',
'DWHLRA',
'DRBKRA',
'DMSCRA',
'DODGRA',
'DJRYRA',
'DTAERA',
'DEBKRA',
'DLUGRA',
'DTCERA',
'DNDGRA',
'DFXARA',
'DTFDRA',
'DAOPRA',
'DFFDRA',
'DCLORA',
'DGARRA',
'DWGCRA',
'DMBCRA',
'DCICRA',
'DOCCRA',
'DGOERA',
'DMFLRA',
'DFULRA',
'DONGRA',
'DPHMRA',
'DREIRA',
'DHOURA',
'DOPCRA',
'DTOBRA',
'DNEWRA',
'DSERRA',
'DHCERA',
'DHUTRA',
'DHSGRA',
'DTENRA',
'DOWNRA',
'DFARRA',
'DGRHRA',
'DUTLRA',
'DWRSRA',
'DELGRA',
'DELCRA',
'DGHERA',
'DHLCRA',
'DOUTRA',
'DPHYRA',
'DDENRA',
'DPMSRA',
'DHPNRA',
'DHSPRA',
'DNRSRA',
'DTRSRA','DMVSRA','DVMRRA','DOVSRA',
'DPUBRA','DGRDRA','DAITRA','DWATRA','DRCARA','DRLSRA','DAVPRA','DGAMRA','DOTRRA','DFSARA','DFSERA','DPMBRA','DFOORA','DACCRA','DIFSRA','DFNLRA','DIMPRA','DOFIRA','DINSRA','DLIFRA','DFINRA','DHINRA','DTINRA','DOTSRA','DCOMRA','DTCSRA','DPSSRA','DINTRA','DTEDRA','DHEDRA','DNEHRA','DVEDRA','DPRSRA','DPERRA','DSOCRA','DHHMRA','DFTRRA']

lvl3a = [x for x in lvl3 if x not in lvl2]