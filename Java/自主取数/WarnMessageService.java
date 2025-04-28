package com.fmys.api.service;

import com.fmys.api.domain.WarnMessage;


public interface WarnMessageService {
    void addRecord(String record);
    String warnStyle4Topics(WarnMessage warnMessage);

}
