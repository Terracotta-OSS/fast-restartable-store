/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.log;

import java.io.IOException;

/**
 *
 * @author mscott
 */
enum LogMachineState {
        BOOTSTRAP {
            LogMachineState progress() {
                return NORMAL;
            }
            
            boolean isBootstrapping() {
                return true;
            }
            
            boolean starting() {
                return true;
            }
            
            boolean acceptRecords() {
                return true;
            }  
        },
        NORMAL {
            LogMachineState shutdown() {
                return SHUTDOWN;
            }
            
            LogMachineState checkException(Exception e) throws RuntimeException {
                if ( e instanceof InterruptedException ) {
                    throw new RuntimeException(e);
                } else {
                    return super.checkException(e);
                }
            }
            
            boolean acceptRecords() {
                return true;
            }           
        },
        SHUTDOWN {
            LogMachineState idle() {
                return FINISHED;
            }
        },
        FINISHED {
            LogMachineState reset() {
                return IDLE;
            }
        },
        ERROR {
            LogMachineState shutdown() {
                return ERROR;
            }
            
            boolean isErrorState() {
                return true;
            }
            
            LogMachineState idle() {
                return FINISHED;
            }
        },
        IDLE {
            LogMachineState bootstrap() {
                return BOOTSTRAP;
            }    
            
            LogMachineState shutdown() {
                return IDLE;
            }    
        
            boolean starting() {
                return true;
            }

            boolean acceptRecords() {
                return true;
            }  
            
            
            
        };
        
        LogMachineState progress() {
            throw new RuntimeException(this + "  -- bad state");
        }
        
        LogMachineState shutdown() {
            throw new RuntimeException(this + "  -- bad state");
        }
        
        LogMachineState idle() {
            throw new RuntimeException(this + "  -- bad state");
        }
        
        LogMachineState bootstrap() {
            throw new RuntimeException(this + "  -- bad state");
        }
        
        LogMachineState reset() {
            throw new RuntimeException(this + "  -- bad state");
        }        
        
        boolean isErrorState() {
            return false;
        }
        
        boolean isBootstrapping() {
            return false;
        }
        
        boolean acceptRecords() {
            return false;
        }
        
        boolean starting() {
            return false;
        }        
        
        LogMachineState checkException(Exception e) throws RuntimeException {
            if ( e instanceof InterruptedException ) {
                return this;
            } else if ( e instanceof IOException ) {
                return ERROR;
            }
            throw new RuntimeException(e);
        }
}
