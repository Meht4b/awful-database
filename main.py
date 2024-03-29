import os
import pickle

class database:

    #creates database if not exists
    def __init__(self,name:str,path=os.getcwd()):
        self.path = path + f'/{name}'
        try:
            os.mkdir(os.getcwd()+f'/{name}')
        except:
            pass
    
    #columns = [[column1,type]..]
    def create_table(self,name:str,columns:list):

        if os.path.isfile(self.table_path(name)):
            raise Exception('table already exists')
        
        f = open(self.table_path(name),'wb')
        pickle.dump(columns,f)
        f.close()

    #writes to table
    def write_table(self,table:str,row:list):
        f = open(self.table_path(table),'+ab')
        f.seek(0)
        rules = pickle.load(f)
        try:
            for i in range(len(rules)):
                if not isinstance(row[i],rules[i][1]):
                    raise Exception('bad input')
            pickle.dump(row,f)    
            
        except:
            raise Exception('bad input')
        f.close()

    #columns = what columns to be displayed        
    def read_table(self,table:str,columns:list) -> list:
        f = open(self.table_path(table),'rb')
        res = []
        names = pickle.load(f)
        names = [x[0] for x in names]
        indices = []

        for i in range(len(names)):
            if names[i] in columns:
                indices.append(i)

        while True:
            try:
                raw = pickle.load(f)
                filtered = []
                for i in indices:
                    filtered.append(raw[i])
                res.append(filtered)
            except EOFError:
                break
        f.close()
        return res

    #shows table structure
    def show_table(self,table:str):
        f = open(self.table_path(table),'rb')
        return pickle.load(f)

    #columns = which columns to check for eg [name,id]
    #condition = what value it should be equal to for eg [name1,01]
    #for item to be deleted all conditions must be satisfied (record with name name1 and id 01 will be deleted)
    def remove_table(self,table:str,columns:list,condition:list):
        f = open(self.table_path(table),'+rb')

        names = pickle.load(f)
        tempdata = [names]
        names = [x[0] for x in names]
        indices = []

        for i in range(len(names)):
            if names[i] in columns:
                indices.append((i,columns.index(names[i])))

        while True:
            try:
                flag = True
                rec = pickle.load(f)
                for i in indices:
                    if rec[i[0]] == condition[i[1]]:
                        flag = False
                    else:
                        flag = True
                        break
                if flag:
                    tempdata.append(rec)
            except EOFError:
                break

        f = open(self.table_path(table),'wb')
        for i in tempdata:
            pickle.dump(i,f)
        f.close()

    #helper function        
    def table_path(self,name):
        return self.path +f'/{name}'
