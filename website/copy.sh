#!/bin/bash
###########################################################################################
##### Copy markdown files from root directory and docs/ to current directory website/ #####
###########################################################################################

# Clean up _docs/ directory
rm -rf _docs

# Clean up _includes/ directory
rm -f _includes/CODE_OF_CONDUCT.md _includes/CONTRIBUTING.md

# Copy CODE_OF_CONDUCT.md and CONTRIBUTING.md to _includes/
rsync -av --ignore-existing ../CODE_OF_CONDUCT.md ../CONTRIBUTING.md _includes/

# Copy ../docs to ./_docs
rsync -av --ignore-existing ../docs/* ./_docs

# Add Front-Matter header to all markdown files in _docs/ or one level deep if it doesn't exist
for file in $(find ./_docs -type f -name "*.md" -maxdepth 2); do
    if ! grep -q "^---" $file; then
        # if the filename is README then add "permalink=directory name" to Front-Matter
        if [[ $(basename $file | tr '[:upper:]' '[:lower:]') == "readme.md" ]]; then
            echo -e "---\npermalink: /$(basename $(dirname $file))/\n---\n\n$(cat $file)" > $file
        else # otherwise just add an empty Front-Matter
            echo -e "---\n---\n\n$(cat $file)" > $file
        fi
    fi
done

# Replace https://armadaproject.io with . in _includes/CONTRIBUTING.md using perl
perl -pi -e 's/https:\/\/armadaproject.io/./g' _includes/CONTRIBUTING.md
