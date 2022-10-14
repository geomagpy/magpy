list='ls *.py'
for el in $list
	do
		if [[ $el != *ls* ]] ; then echo $el; nel=`echo $el |sed 's/py/mycode/g'`; `sed '/^#/d' $el > $nel`; fi
	done

