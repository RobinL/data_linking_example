
def get_test_data_rules():
    rules = []

    rules.append('l.first_name = r.first_name  and  l.surname = r.surname')
    rules.append('l.first_name = r.first_name  and  l.dob = r.dob')
    rules.append('l.first_name = r.first_name  and  l.city = r.city')
    rules.append('l.first_name = r.first_name  and  l.email = r.email')

    rules.append('l.surname = r.surname  and  l.dob = r.dob')
    rules.append('l.surname = r.surname  and  l.city = r.city')
    rules.append('l.surname = r.surname  and  l.email = r.email')

    rules.append('l.dob = r.dob  and  l.city = r.city')
    rules.append('l.dob = r.dob  and  l.email = r.email')

    rules.append('l.city = r.city  and  l.email = r.email')

    # rules.append(
    #     'l.first_name = r.first_name  and  l.surname = r.surname and  l.dob = r.dob')

    # rules.append(
    #     'l.first_name = r.first_name  and  l.surname = r.surname and  l.email = r.email')


    return rules
