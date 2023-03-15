from magpy.absolutes import *
import numpy as np



def readAUTODIFABS(file_name: string, headonly: bool = False, **kwargs) -> list:
    output = kwargs.get('output')
    if output != 'DIListStruct':
        raise NotImplementedError
    try:
        abs_list = []
        scale_flux = kwargs.get('scaleflux', 0.098)
        scale_angle = kwargs.get('scaleangle', 0.00011)
        temperature = kwargs.get('temperature', float(nan))
        legacy = True
        with open(file_name, 'r', encoding='ascii') as file:
            lines = file.readlines()
        first_word = lines[0].split(':')[0].strip()
        if first_word == 'STATION NAME':
            legacy = False
        if legacy:
            di_inst = lines[0].split('\t')[0]
            iaga = lines[3].split(":")[1].strip()
            pier = lines[4].split(":")[1].strip()
            target_id = lines[5].split(":")[1].strip()
            azimuth = float(lines[6].split(":")[1].strip())
        else:
            di_inst = lines[7].split(":")[1].strip()
            iaga = lines[1].split(":")[1].strip()
            target_id = lines[4].split(":")[1].strip()
            azimuth = float(lines[6].split(":")[1].strip())
            pier = lines[5].split(":")[1].strip()
        person = di_inst
        i = 10
        while i < len(lines):
            line = lines[i][:-1]
            if line.upper().startswith('RECTIME') and line.upper().endswith('COMPLETE'):
                di_to_add = _get_di_line_struct_complete(lines[i:i + 13],legacy)
                di_to_add.person = person
                di_to_add.azimuth = azimuth
                di_to_add.di_inst = di_inst
                di_to_add.pier = pier
                di_to_add.stationid = iaga
                di_to_add.scaleflux = scale_flux
                di_to_add.scaleangle = scale_angle
                abs_list.append(di_to_add)
                i = i + 13
            else:
                if line.upper().startswith('RECTIME') \
                        and line.upper().endswith('MAGNETIC'):
                    #to do what if there is no complete recording ?
                    i = i + 9
                else:
                    i = i + 1

        return abs_list

    except Exception as e:
        loggerabs.error(e)
        raise e


def _get_di_line_struct_complete(complete_mes,legacy: bool) -> DILineStruct:
    di_line: DILineStruct = DILineStruct(24)
    #fill in timestamp/hc  for TU TD UE DW DE UW
    for i in range(0,len(complete_mes)-5):
            di_line.time[2*i] = _get_timestamp(complete_mes[i+1])
            di_line.time[2 * i + 1] = _get_timestamp(complete_mes[i+1])
            di_line.hc[2*i] = float(complete_mes[i+1].split('\t')[3].strip())
            di_line.hc[2*i+1] = float(complete_mes[i+1].split('\t')[3].strip())
    di_line.hc[-8:] =  [180.0, 180.0, 0.0, 0.0, 180.0, 180.0, 0.0, 0.0]
    # fill in timestamp/vc  for US DN DS UN
    for j in range(8, len(complete_mes)-1):
        if legacy:
            index = j + 1
        else:
            #inverse order for new format
            index = len(complete_mes)-j + 7
        di_line.time[2 * j] = _get_timestamp(complete_mes[index])
        di_line.time[2 * j + 1] = _get_timestamp(complete_mes[index])
        di_line.vc[2 * j] = float(complete_mes[index].split('\t')[3].strip())
        di_line.vc[2 * j + 1] = float(complete_mes[index].split('\t')[3].strip())
    # declination vc
    di_line.vc[4:12] = [90.0, 90.0, 270.0, 270.0, 270.0, 270.0, 90.0, 90.0]
    di_line.res = 24 * [0.0]
    return di_line


def _get_timestamp(str_mes) -> np.datetime64:
        words = str_mes.split('\t')
        return date2num(datetime.strptime(words[1] + " " + words[2], '%Y-%m-%d %H:%M:%S'))
