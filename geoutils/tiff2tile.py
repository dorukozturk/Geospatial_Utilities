import gdal
import gdal_retile

def tiff2tile(tiff_file, output_dir):
    args = ['gdal_retile.py', tiff_file, '-of', 'GTiff', '-ps', '256', '256', '-targetDir', output_dir]
    gdal_retile.main(args)

    return "Finished"

if __name__ == '__main__':
    tiff_file = "input_tiff.tiff"
    output_dir = "output_directory"
    tiff2tile(tiff_file, output_dir)
