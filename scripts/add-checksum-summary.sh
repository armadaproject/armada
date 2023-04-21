API_TOKEN=$1
RELEASE_TAG=$2

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

BODY=$(echo $RELEASE_INFO_REPLY | jq '.body')

if [ "$BODY" = null ]; then
    echo "Failed to get body of release with tag $RELEASE_TAG"
    exit -1
fi

BODY="${BODY:1:${#BODY}-2}"
echo $BODY

DETAILS="$BODY"
DETAILS="${DETAILS}\n# Checksums"

for artifact in ./dist/*; do
   SHA=$(sha256sum "${artifact}"  | cut -d " " -f 1)
   DETAILS="${DETAILS}\n"
   DETAILS="${DETAILS}* $(basename $artifact) SHA256 \`$SHA\`"
done

JSON_STRING="{\"body\" : \"$DETAILS\"}"

echo "$JSON_STRING"


curl -H "Authorization: token $API_TOKEN" -X PATCH https://api.github.com/repos/armadaproject/armada/releases/$RELEASE_ID --data "$JSON_STRING"
