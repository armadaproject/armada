API_TOKEN=$1
RELEASE_TAG=$2
FILE=$3

if [ ! -f $FILE ]; then
    echo "Could not find file to upload, $FILE is not a valid file path"
    exit -1
fi

RELEASE_INFO_REPLY=$(curl --fail -H "Authorization: token $API_TOKEN" https://api.github.com/repos/armadaproject/armada/releases/tags/$RELEASE_TAG)

if [ $? -ne 0 ]; then
    echo "Failed to get release information from Github"
    exit -1
fi

RELEASE_ID=$(echo $RELEASE_INFO_REPLY | jq -r '.id')

if [ $RELEASE_ID = null ]; then
    echo "Failed to get id of release with tag $RELEASE_TAG"
    exit -1
fi

curl --fail -H "Authorization: token $API_TOKEN" -H "Content-Type: application/octet-stream" --data-binary @$FILE "https://uploads.github.com/repos/armadaproject/armada/releases/$RELEASE_ID/assets?name=$(basename $FILE)"

if [ $? -ne 0 ]; then
    echo "Failed to upload file to Github"
    exit -1
fi
