def gen_sql_dict(k):
    with open('tweet_extract_mapping.txt', 'r') as f:
        content = f.readlines()
    lines = [x.strip() for x in content]
    sql_col_str=''
    sql="insert into tweet_details( \n"
    sql_val_str=" values( "
    field_count=len(lines)
    print(field_count)
    l_count=1
    for j in lines:
        t=j.split(",")
        if l_count<field_count:
            sql_col_str=sql_col_str+str(t[0])+",\n"
            l_count=l_count+1
            try:
                sql_val_str=sql_val_str+"'"+str(k[str(t[2]).strip()]).replace("'","\\'")+"',\n"
            except:
                sql_val_str=sql_val_str+"'"+"',\n"
                

        elif l_count==field_count:
            sql_col_str=sql_col_str+str(t[0])+")"
            l_count=l_count+1
            sql_val_str = sql_val_str +"'"+ str(k[str(t[2]).strip()]).replace("'","\\'") + "')\n"

    return(sql+sql_col_str+sql_val_str)
