#!/bin/bash
declare cwd=`pwd`
declare hooks_path=$cwd/hooks
declare git_hooks_path=$cwd/.git/hooks

echo "##############################################"
echo "Installing git hooks..."

echo "Moving old pre-commit file"
mv $git_hooks_path/pre-commit $git_hooks_path/pre-commit.old

echo "Installing pre-commit file"
pre-commit install

echo "Creating symlink to prepare-commit-msg in the .git/hooks folder"
chmod +x $hooks_path/prepare-commit-msg
rm -f $git_hooks_path/prepare-commit-msg

# Employing an absolute path solves the issue of too many levels of symbolic links
ln -s $hooks_path/prepare-commit-msg $git_hooks_path
echo "##############################################"
