# my fixes on buttonrpc.hpp
1. change parameters "std::string name" into "const std::string& name"
Explanition: 不是为了性能，因为文件name和ip都是短字符串，所以上述两者性能几乎无差异。主要是为了语义的正确性，因为传值基本上默认会对这个值做出改变，而这里不会改变参数的函数，应该用"const std::string name"(这个用法不常见也不推荐) or "const std:string& name"