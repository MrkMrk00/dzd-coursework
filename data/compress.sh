#!/usr/bin/bash

if ! which zstd > /dev/null 2>&1 ; then
	echo "Nemam zstd :/"
	echo "nainstaluj: https://github.com/facebook/zstd"

	exit 1
fi

if ! which parallel > /dev/null 2>&1 ; then
	echo "Nemam GNU parallel :/"
	echo "nainstaluj: https://www.gnu.org/software/parallel/"

	exit 1
fi

CHECKSUM="05f063ffe73827940f97a3705aef4017"

compress() {
	split --bytes=50M daily_weather.parquet dw-part-
	FILES=$(ls | grep -E '(.csv)|(dw-part-)')
	
	[[ -d ./compressed ]] || mkdir ./compressed
	
	parallel --lb 'zstd -f -19 {} -o "compressed/{}.zst"' ::: $FILES
}

decompress() {
	FILES=$(ls ./compressed)

	parallel --lb 'zstd -d -f "./compressed/{}" -o "./{.}"' ::: $FILES

	cat dw-part-* > daily_weather.parquet

	NEW_CHECKSUM=$(md5sum --binary daily_weather.parquet | cut -d' ' -f1)
	[[ $CHECKSUM == $NEW_CHECKSUM ]] && echo "daily_weather checksum OK" || (echo "daily_weather checksum FAIL"; exit 1)
}

case "$1" in
	"compress")
		compress
	;;

	"decompress")
		decompress	
	;;

	*)
		echo "Unknown option"
	;;
esac

