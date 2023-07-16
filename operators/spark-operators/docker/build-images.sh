export imagename='spark-operators'

DIR=$(cd `dirname $0`; pwd)

cd "$DIR"/.. || exit

cd "$DIR" || exit

mkdir -p ./cache

cp -r "$DIR"/../docker/spark-operators ./cache/spark-operators
echo "Image name $imagename"

docker build --no-cache --tag "${imagename}" -f "$DIR"/Dockerfile "$DIR"
