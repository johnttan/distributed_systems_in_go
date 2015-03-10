for run in {0...10};
  do go test >>& finallogs.txt;
done
