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

split_compressed_daily_weather() {
	pushd . && cd compressed

	split --bytes=48M daily_weather.parquet.zst dw-part-
	rm daily_weather.parquet.zst

	popd
}

combine_compressed_daily_weather() {
	pushd . && cd compressed

	cat dw-part-* > daily_weather.parquet.zst
	rm dw-part-*

	popd
}

weather_checksum() {
	NEW_CHECKSUM=$(md5sum --binary daily_weather.parquet | cut -d' ' -f1)
	[[ $CHECKSUM == $NEW_CHECKSUM ]] && echo "daily_weather checksum OK" || (echo "daily_weather checksum FAIL"; exit 1)
}

compress() {
	FILES=$(ls | grep -E '(\.csv)|(\.parquet)')
	
	[[ -d ./compressed ]] || mkdir ./compressed
	
	parallel --lb 'zstd -f {} -o "compressed/{}.zst"' ::: $FILES
	split_compressed_daily_weather
}

decompress() {
	combine_compressed_daily_weather

	FILES=$(ls ./compressed)

	parallel --lb 'zstd -d -f "./compressed/{}" -o "./{.}"' ::: $FILES

	cat dw-part-* > daily_weather.parquet

	weather_checksum
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

