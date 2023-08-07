# my fixes on buttonrpc.hpp
1. change parameters "std::string name" into "const std::string& name"
Explanition: 不是为了性能，因为文件name和ip都是短字符串，所以上述两者性能几乎无差异。主要是为了语义的正确性，因为传值基本上默认会对这个值做出改变，而这里不会改变参数的函数，应该用"const std::string name"(这个用法不常见也不推荐) or "const std:string& name"

2. 原始版本在serializer.hpp头文件中的全局位置使用了using namespace std;(离谱，污染了整个命名空间)

3. 将多个函数重载合并为一个可变参数函数模板

4. 增加了对const成员函数的支持，原始版本不支持 (也是有点离谱)

5. 原始版本的 Serializer 类的实现是通过继承std::vector<char>, 然而继承标准库容器不是一个好的做法，修改为组合实现。
Explaination: 
1) 标准库容器的析构函数不是虚函数，如果使用基类指针或引用来删除一个派生类对象，那么将不会调用基类的析构函数；
2) 且Serializer并没有修改std::vector的任何行为，如果只是想添加额外的功能，那么 better 组合 than 继承 