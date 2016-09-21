# Geospatial_Utilities

To extract bands 3,2,1 with this order(True Color)
```sh
hdf2tiff -b 3,2,1 L57.Globe.month07.2011.hh09vv04.h6v1.doy182to212.NBAR.v3.0.hdf
```

To extract bands 4,3,2 with this order(False Color)
hdf2tiff -b 4,3,2 L57.Globe.month07.2011.hh09vv04.h6v1.doy182to212.NBAR.v3.0.hdf
```

To get a single band 9 (NDVI)
```sh
hdf2tiff -b 9 L57.Globe.month07.2011.hh09vv04.h6v1.doy182to212.NBAR.v3.0.hdf
```

To convert all the hdf files to tiffs
```sh
hdf2tiff -b 3,2,1,9 *.hdf
```

To overwrite existing tiff files
```sh
hdf2tiff -b 3,2,1,9 --clobber *.hdf
```

To specify a different output directory
```sh
hdf2tiff -b 3,2,1 --clobber -o some/dir *.hdf
```