from sklearn.linear_model import LinearRegression
import pandas as pd
import tkinter as tk
import math


def Reg():
    z = zn.get()
    m = mon.get()
    m = float(m)
    h = hr.get()
    h = float(h)
    temp = Temp.get()
    temp = float(temp)
    w = Wind.get()
    w = float(w)
    preci = Preci.get()
    preci = float(preci)
    locid = locdata[locdata['Zone'] == z]
    lid = locid['LocationID'].iloc[0]
    tlist = [[lid, h, m, temp, w, preci]]
    test = pd.DataFrame(tlist, columns=['PULocationID', 'Pick up Hour', 'Pick up Month', 'tempi', 'wspdi', 'precipi'])

    # read and create training dataframe

    Y_train = traindata['count(id)']
    X_train = traindata.drop('count(id)', axis=1)
    lm = LinearRegression()
    lm.fit(X_train, Y_train)
    test['PredictedPickup'] = lm.predict(test)
    pred = math.ceil(test['PredictedPickup'].iloc[0])
    if pred < 0:
        pred = 0

    t = tk.Label(master, text="Predicted Pickups: " + str(pred))
    t.grid(row=7, sticky=tk.W)


traindata = pd.read_csv('Q1_PythnDataset.csv', sep=';')
locdata = pd.read_csv('taxi+_zone_lookup.csv')

master = tk.Tk(className='Yellow Taxi Pickup Predictor')
master.geometry("300x300")

Zonelist = locdata['Zone']
Hour = list(set(traindata['Pick up Hour']))
Month = list(set(traindata['Pick up Month']))

t = tk.Label(master, text="Select Pickup Month: ")
t.grid(row=0, sticky=tk.W)

mon = tk.StringVar(master)
mon.set(Month[0])
r = tk.OptionMenu(master, mon, *Month)
r.grid(row=0, column=1, columnspan=3, sticky=tk.W)

t = tk.Label(master, text="Select Pickup Hour: ")
t.grid(row=1, sticky=tk.W)

hr = tk.StringVar(master)
hr.set(Hour[0])
r = tk.OptionMenu(master, hr, *Hour)
r.grid(row=1, column=1, columnspan=3, sticky=tk.W)

t = tk.Label(master, text="Select Zone: ")
t.grid(row=2, sticky=tk.W)

zn = tk.StringVar(master)
zn.set(Zonelist[0])
r = tk.OptionMenu(master, zn, *Zonelist)
r.grid(row=2, column=1, columnspan=3, sticky=tk.W)

t = tk.Label(master, text="Enter Temperature: ")
t.grid(row=3, sticky=tk.W)

Temp = tk.Entry(master)
Temp.grid(row=3, column=1, columnspan=3, sticky=tk.W)

t = tk.Label(master, text="Enter Precipitation: ")
t.grid(row=4, sticky=tk.W)

Preci = tk.Entry(master)
Preci.grid(row=4, column=1, columnspan=3, sticky=tk.W)

t = tk.Label(master, text="Enter Windspeed: ")
t.grid(row=5, sticky=tk.W)

Wind = tk.Entry(master)
Wind.grid(row=5, column=1, columnspan=3, sticky=tk.W)

s = tk.Button(master, text="Predict", command=Reg)
s.grid(row=6, column=1, sticky=tk.W)

master.grid_columnconfigure(11, minsize=70)
master.grid_rowconfigure(15, minsize=50)
master.mainloop()