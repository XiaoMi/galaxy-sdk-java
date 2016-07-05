Release process

# update change log
# modify pom version to release version : e.g., sed -i "s/1.2-SNAPSHOT/1.1.3/g" `find . -name "pom.xml"| xargs grep "1.2-SNAPSHOT" -l`
# git tag for release version
# release to maven central, do not forget to release it manually in oss.sonatype.org
# modify pom version to next snapshot version : e.g., sed -i "s/1.1.3/1.2-SNAPSHOT/g" `find . -name "pom.xml"| xargs grep "1.1.3" -l`
# git push sdk to github remote lib if demo having change
