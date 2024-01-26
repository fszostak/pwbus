#!/usr/bin/env bash
# Fabio Szostak
# Wed Apr 22 02:20:02 UTC 2020

trap abort INT

abort()
{
    sed -i "s/version=\"$NEW_VERSION\"/version=\"$VERSION\"/" setup.py
    sed -i "s/__version__ = \"$NEW_VERSION\"/__version__ = \"$VERSION\"/" src/pwbus/cli/command_line.py
    echo "Aborted!!! Version reverted to $VERSION"
    exit
}

VERSION=$(grep 'version=' setup.py | cut -f2 -d\")
IFS=.
set - $VERSION
let V=$3+1
NEW_VERSION=$1.$2.$V
IFS=" "

echo "pwbus: $VERSION => $NEW_VERSION"
sed -i "s/version=\"$VERSION\"/version=\"$NEW_VERSION\"/" setup.py
sed -i "s/__version__ = \"$VERSION\"/__version__ = \"$NEW_VERSION\"/" src/pwbus/cli/command_line.py

echo "Press ENTER to continue or CTRL-C to abort"
read ENTER

echo "Installing..."
pip3 install -r requirements.txt > /dev/null 2>&1
pip3 install -r requirements.txt > /dev/null 2>&1
[ $? -ne 0 ] && abort

pip3 install -e . > /dev/null 2>&1
[ $? -ne 0 ] && abort

echo "Building..."
python3 setup.py sdist bdist_wheel
RET=$?

echo "Deploying..."
if [ $RET -eq 0 ]; then
    python3 -m twine upload dist/*
    RET=$?
fi

echo

if [ $RET -ne 0 ]; then
		python3 -m pip install --user --upgrade twine
    abort
fi
