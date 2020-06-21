#! /bin/bash

#就这么简单
word=`xclip -out`
echo $word
mea=`sdcv -n ${word}`
echo $mea
mean=`sdcv -n ${word} | grep "^[a-z]"`
echo $mean

pkill notify-osd
notify-send   "$mean"
