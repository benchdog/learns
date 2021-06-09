#coding: utf8
from flask import Flask, render_template, request
import json

app = Flask(__name__)
@app.route('/VIID/SubscribeNotifications', methods=['POST'])
def index():
    # print(request.headers)
    # print(json.dumps(request.form))
    print(request.data.decode('utf8'))
    print('------------')
    return 'ok'


@app.route('/p')
@app.route('/p/<name>')

def hello(name=None):
    return render_template('p.html', name=name)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=80, debug=True)  # debug = True 设置调试模式，生产模式的时候要关掉debug


'''
1.  request.scheme : 获取请求协议
2.  request.method : 获取本次请求的请求方式(GET / POST)
3.  request.args : 获取以get请求方式提交的数据
4.  request.form : 获取以post请求方式提交的数据
5.  request.cookies:获取cookies中的相关信息
6.  request.headers：获取请求信息头的相关信息
7.  request.files：获取上传的文件
8.  request.path：获取请求的资源具体路径(不带参数)
9.  request.full_path：获取完整的请求资源具体路径(带参数)
10. request.url: 获取完整的请求地址，从协议开始
11. request.files 获取上传的文件用（save进行保存）
里面要注意低是arequest.args，request.form ，request.files 的返回值都是字典。
响应就是服务器响应给客户端的内容
响应的都是字符串， html文件也是字符串
'''