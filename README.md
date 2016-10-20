# CS 186 Fall 2016

Course repository for [CS 186, Fall 2016](http://cs186berkeley.net). All
homeworks and projects will be released here. Please use this repository to
get the starter code for the projects. If you have any questions, see hw0 or
post on [Piazza](https://piazza.com/class/is0phopc27275j).

# Git configuration

    git remote add berkeley https://github.com/berkeley-cs186/course
    git pull berkeley master

# Debug

    mvn test -Dtest=TestTable -Dmaven.surefire.debug test
    jdb -attach 5005 --sourcepath ./src/main/java:./src/test/java

All course announcements will be on the course website and on Piazza.
