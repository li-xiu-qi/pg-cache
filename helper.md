___
python setup.py sdist bdist_wheel
___

pip install twine
___

twine upload dist/*
___

___

# 生成分发文件

python setup.py sdist bdist_wheel

# 上传到 PyPI

twine upload dist/*
___
