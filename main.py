from flask import Flask, render_template, json, request, redirect, url_for, Response
from pymongo import MongoClient, TEXT
import pandas as pd
import os
import requests
from datetime import date

app = Flask(__name__)
USER = "grupo3"
PASS = "grupo3"
DATABASE = "grupo3"
URL = f"mongodb://{USER}:{PASS}@gray.ing.puc.cl/{DATABASE}"
CLIENT = MongoClient(URL)
DB = CLIENT["grupo3"]
DB_USERS = DB.users
DB_MSGS = DB.messages

# GET: /
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sendMessages')
def sendMessage():
    return render_template('sendMessage.html')

# GET: /messages
@app.route("/messages")
def get_messages():
    uid1 = request.args.get("id1")
    uid2 = request.args.get("id2")
    if uid1 and uid2:
        user1 = list(DB_USERS.find({"uid": int(uid1)}, {"_id": 0}))
        user2 = list(DB_USERS.find({"uid": int(uid2)}, {"_id": 0}))
        if user1 == [] and user2 == []:
            return json.jsonify({'HTTP 404 Not Found' : "Neither {}, nor {} exist.".format(uid1, uid2)}), 404
        elif user1 == []:
            return json.jsonify({'HTTP 404 Not Found' : "Unexisting user with id : {}.".format(uid1)}), 404
        elif user2 == []:
            return json.jsonify({'HTTP 404 Not Found' : "Unexisting user with id : {}.".format(uid2)}), 404
        else:
            msgs1 = list(DB_MSGS.find({"sender": int(uid1), "receptant": int(uid2)}, {"_id": 0}))
            msgs2 = list(DB_MSGS.find({"sender": int(uid2), "receptant": int(uid1)}, {"_id": 0}))
            return json.jsonify(msgs1 + msgs2)

    elif uid1:
        return json.jsonify({'HTTP 400 Bad Request' : "Given (1) argument. Expecting (2)."}), 400
    elif uid2:
        return json.jsonify({'HTTP 400 Bad Request' : "Given (1) argument. Expecting (2)."}), 400
    else: 
        messages = list(DB_MSGS.find({}, {"_id": 0}))
        return json.jsonify(messages)
# GET: /messages/<int:mid>
@app.route("/messages/<int:mid>")
def get_message(mid):
    msg = list(DB_MSGS.find({"mid": mid}, {"_id": 0}))
    if msg == []:
        return json.jsonify({'HTTP 404 Not Found' : "Unexisting message."}), 404
    else:
        return json.jsonify(msg)

# GET: /users 
@app.route("/users")
def get_users():
    users = list(DB_USERS.find({}, {"_id": 0}))
    return json.jsonify(users)

# GET: /users/send/<int:uid>
@app.route("/users/send/<int:uid>")
def get_user_message(uid):
    user = list(DB_USERS.find({"uid": uid}, {"_id": 0}))
    if user == []:
        return json.jsonify({'HTTP 404 Not Found' : "Unexisting user."}), 404
    else:
        messages = list(DB_MSGS.find({"sender": uid}, {"_id": 0}))
        return json.jsonify(user + messages)

# GET: /users/receptant/<int:uid>
@app.route("/users/receptant/<int:uid>")
def get_receptant_message(uid):
    user = list(DB_USERS.find({"uid": uid}, {"_id": 0}))
    if user == []:
        return json.jsonify({'HTTP 404 Not Found' : "Unexisting user."}), 404
    else:
        messages = list(DB_MSGS.find({"receptant": uid}, {"_id": 0}))
        return json.jsonify(user + messages)

# POST: /messages/MessageJson
@app.route("/messages", methods=['POST'])
def createMessage():
    # get_json()
    requestMessage = request.form.to_dict(flat=False)
    requestMessage['date'] = str(date.today())
    requestMessage['lat'] = '-10000'
    requestMessage['long'] = '10000'
    requestMessage['sender'] = int(requestMessage['sender'][0])
    requestMessage['message'] = requestMessage['message'][0]
    receptant = getUidReceptant(requestMessage['receptant'], DB_USERS)
    if isinstance(receptant, str):
        return json.jsonify({'HTTP 404 Not Found': receptant}), 404
    else:
        requestMessage['receptant'] = receptant['uid']
    errorMessage = dataErrors(requestMessage, DB_USERS)
    if isinstance(errorMessage, list):
        return json.jsonify({'HTTP 404 Not Found': errorMessage}), 404
    requestMessage['mid'] = getMsgId(DB_MSGS)
    DB_MSGS.insert_one(requestMessage)
    return json.jsonify({'HTTP 200 OK' : f'Message successfully inserted'}), 200

def dataErrors(request, DB_USERS):
    messageKeys = ['message', 'sender', 'receptant', 'lat', 'long']
    missingKeys = set()
    for key in messageKeys:
        if not(key in request):
            missingKeys.add(key)
        elif request[key] == '': 
            missingKeys.add(key)
    if len(missingKeys) >= 1:
        return f'Unexisting {", ".join(missingKeys)}'
    sender = getUser(request['sender'], DB_USERS)
    receptant = getUser(request['receptant'], DB_USERS)
    if not(sender):
        if not(receptant):
            return f'Invalid sender: {request["sender"]}, Invalid receptant: {request["receptant"]}'
        return f'Invalid sender: {request["sender"]}'
    elif not(receptant):
        return f'Invalid receptant: {request["receptant"]}'
    return None

def getUidReceptant(name, DB_USERS):
    user = list(DB_USERS.find({"name": name[0]}, {"_id": 0}))
    if user == []:
        return f'Invalid receptant: {name}'
    return user[0]

def getUser(uid, DB_USERS):
    user = list(DB_USERS.find({"uid": uid}, {"_id": 0}))
    if user == []:
        return False
    return True

def getMsgId(DB_MSGS):
    listMessages = list(DB_MSGS.find({}, {"_id": 0}))
    mids = set()
    for dataMessage in listMessages:
        mids.add(dataMessage['mid'])
    for number in range(1, len(listMessages) + 1):
        if not(number in mids):
            return number
    return len(listMessages) + 1

# DELETE: /messages/<int:mid>
@app.route("/messages/<int:mid>", methods=['DELETE'])
def delete_user_message(mid):
    if list(DB_MSGS.find({"mid": mid}, {"_id": 0})) == []:
        return json.jsonify({'HTTP 404 Not Found' : "Unexisting message."}), 404
    else:
        DB_MSGS.delete_one({"mid": mid})
        return json.jsonify({'HTTP 200 OK' : "Message deletion succesful."}), 200

@app.route("/text-search", methods=['GET', 'POST'])
def textsearch():
    required = []
    forbidden = []
    desired = []
    user_id = []
    # get_json()
    if request.method == 'POST':
        parameters = request.form.to_dict(flat=False)
        if parameters:
            required = parameters['required'][0].split(', ') if not(parameters['required'][0] in "") else []
            forbidden = parameters['forbidden'][0].split(', ') if not(parameters['forbidden'][0] in "") else []
            desired = parameters['desired'][0].split(', ') if not(parameters['desired'][0] in "") else []
            user_id = parameters['userId'][0] if not(parameters['userId'][0] in "") else []
        filtered = " ".join([*(f"\"{s}\"" for s in required), *(f"{s}" for s in desired),
        *(f"-{s}" for s in forbidden)])
        flag = getUser(user_id, DB_USERS) #True si existe, False e.o.c.
        DB_MSGS.create_index([('message', TEXT)])
        if flag:
            if forbidden != []:
                if desired != []:
                    if required != []:
                        #Entregan: forbidden + desired + user_id + required
                        result = DB_MSGS.find({"$text": {"$search": filtered}, "sender":user_id}, {"_id": 0})
                        output = [msg for msg in result]
                    elif required == []:
                        #Entregan: forbidden + desired + user_id
                        result = DB_MSGS.find({"$text": {"$search": filtered}, "sender":user_id}, {"_id": 0})
                        output = [msg for msg in result]
                elif desired == []:
                    if required != []:
                        #Entregan: forbidden + required + user_id
                        result = DB_MSGS.find({"$text": {"$search": filtered}, "sender":user_id}, {"_id": 0})
                        output = [msg for msg in result]
                    elif required == []:
                        #Entregan: forbidden + user_id
                        filtrar = DB_MSGS.find({'sender':user_id}, {"_id": 0})
                        todos_msj = [msg for msg in filtrar]
                        output = forbidden_function(todos_msj, forbidden)
            elif forbidden == []:
                if desired != []:
                    if required != []:
                        #Entregan: user_id + desired + required
                        result = DB_MSGS.find({"$text": {"$search": filtered}, "sender":user_id}, {"_id": 0})
                        output = [msg for msg in result]
                    elif required == []:
                        #Entregan: user_id + desired
                        result = DB_MSGS.find({"$text": {"$search": filtered}, "sender":user_id}, {"_id": 0})
                        output = [msg for msg in result]
                elif desired == []:
                    if required != []:
                        #Entregan: user_id + required
                        result = DB_MSGS.find({"$text": {"$search": filtered}, "sender":user_id}, {"_id": 0})
                        output = [msg for msg in result]
                    elif required == []:
                        #Entregan: user_id
                        result = DB_MSGS.find({"sender": user_id}, {"_id": 0})
                        output = [msg for msg in result]
        elif not(flag) and user_id == []:
            if forbidden != []:
                if desired != []:
                    if required != []:
                        #Entregan: forbidden + required + desired
                        result = DB_MSGS.find({"$text": {"$search": filtered}}, {"_id": 0})
                        output = [msg for msg in result]
                    elif required == []:
                        #Entregan: forbidden + desired
                        result = DB_MSGS.find({"$text": {"$search": filtered}}, {"_id": 0})
                        output = [msg for msg in result]
                elif desired == []:
                    if required != []:
                        #Entregan: forbidden + required
                        result = DB_MSGS.find({"$text": {"$search": filtered}}, {"_id": 0})
                        output = [msg for msg in result]
                    elif required == []:
                        #Entregan: forbidden
                        filtrar = DB_MSGS.find({}, {"_id": 0})
                        todos_msj = [msg for msg in filtrar]
                        output = forbidden_function(todos_msj, forbidden)
            elif forbidden == []:
                if desired != []:
                    if required != []:
                        #Entregan: desired + required
                        result = DB_MSGS.find({"$text": {"$search": filtered}}, {"_id": 0})
                        output = [msg for msg in result]
                    elif required == []:
                        #Entregan: desired
                        result = DB_MSGS.find({"$text": {"$search": filtered}}, {"_id": 0})
                        output = [msg for msg in result]
                elif desired == []:
                    if required != []:
                        #Entregan: required
                        result = DB_MSGS.find({"$text": {"$search": filtered}}, {"_id": 0})
                        output = [msg for msg in result]
                    elif required == []:
                        #Entregan: Nada
                        result = DB_MSGS.find({}, {"_id": 0}) 
                        output = [msg for msg in result]
        else:
            result = {} #Entregan: user_id que no existe
            output = [msg for msg in result]
    return json.jsonify(output)

def forbidden_function(messages, forbidden):
    dicts_to_delete = []
    for d in messages:
        try:
            if any(f in d["message"] for f in forbidden):
                dicts_to_delete.append(d)
        except TypeError:
            pass
    for d in dicts_to_delete:
        if d in messages:
            messages.remove(d)
    return messages

        
if __name__ == "__main__":
    app.run(debug=True)

