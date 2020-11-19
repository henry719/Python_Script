
sql = "select max((own_card_click_num/total_station_exposure_pv)*((page_stay_num*0.05+search_action_num*2+recommend_card_click_num*1+recommend_service_card_click_num*2+indic_num*3+comment_num*4+reply_num*4+own_service_card_click_num*3+immediately_consult_num*3+video_interview_click_num*3+manual_consultation_click_num*3)/content_detail_page_pv)) as max_score from al_community_content_detail_smart_rank_v1 where create_date='" + str(
    day) + "' and card_type=" + "'user_post'";


sql = "select min((own_card_click_num/total_station_exposure_pv)*((page_stay_num*0.05+search_action_num*2+recommend_card_click_num*1+recommend_service_card_click_num*2+indic_num*3+comment_num*4+reply_num*4+own_service_card_click_num*3+immediately_consult_num*3+video_interview_click_num*3+manual_consultation_click_num*3)/content_detail_page_pv)) as min_score from al_community_content_detail_smart_rank_v1 where create_date='" + str(
    day) + "' and card_type=" + "'user_post'";




sql = "select (own_card_click_num/total_station_exposure_pv)*((page_stay_num*0.05+search_action_num*2+recommend_card_click_num*1+recommend_service_card_click_num*2+indic_num*3+comment_num*4+reply_num*4+own_service_card_click_num*3+immediately_consult_num*3+video_interview_click_num*3+manual_consultation_click_num*3)/content_detail_page_pv) as cur_score from al_community_content_detail_smart_rank_v1 where create_date='" + str(
    day) + "' and card_type=" + "'user_post'" + " and card_id = '" + str(card_id) + "'";


business_weight = 0.5 if if_business_item else 0.0
total_score = ((card_content_level - 3.0) / (6 - 3)) * 100 * (1.0 - ratio) + \
              ((cur_good_click_score - min_good_click_score) / (
                      max_good_click_score - min_good_click_score)) * 100 * (1.0 + business_weight) * ratio



max_good_click_score = 186.04591836734693
min_good_click_score = 0



def gauss(cur_val, ori_val, offset, scal, decay):
    return np.exp(-math.pow(max(0, abs(cur_val - ori_val) - offset), 2) / (
            2 * math.pow((-math.pow(scal, 2) / (2 * math.log1p(decay))), 2)))




ratio = gauss(10 - 9, 0, 0, 1, 0.2)



np.exp(-math.pow(277 - 9, 2) / ( 2 * math.pow((-math.pow(1, 2) / (2 * math.log1p(0.2))), 2)))






