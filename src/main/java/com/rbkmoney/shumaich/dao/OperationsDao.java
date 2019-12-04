package com.rbkmoney.shumaich.dao;

import org.springframework.stereotype.Component;

@Component
public class OperationsDao {

//    @Autowired
//    private RedisTemplate<String, Object> redisObjectTemplate;
//
//    // inject the template as ListOperations
//    @Resource(name="redisObjectTemplate")
//    private ListOperations<String, Object> listOps;
//
//    private void usefulExample() {
//        //execute a transaction
//        List<Object> txResults = redisObjectTemplate.execute(new SessionCallback<List<Object>>() {
//            public List<Object> execute(RedisOperations operations) throws DataAccessException {
//                operations.multi();
//                operations.opsForSet().add("key", "value1");
//
//                // This will contain the results of all operations in the transaction
//                return operations.exec();
//            }
//        });
//    }
}
