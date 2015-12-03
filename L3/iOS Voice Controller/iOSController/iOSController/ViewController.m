//
//  ViewController.m
//  iOSController
//
//  Created by Mayanka  on 9/9/15.
//  Copyright (c) 2015 umkc. All rights reserved.
//

#import "ViewController.h"

@interface ViewController ()
@property (strong, nonatomic) IBOutlet UITextField *ipAddress;
@property (strong, nonatomic) IBOutlet UITextField *portNumber;

@end

// Create a Account in the following http://nuancemobiledeveloper.com/public/index.php?task=home for Application keys and other details

const unsigned char SpeechKitApplicationKey[] = {0xbf, 0x98, 0x6a, 0x4b, 0xb6, 0xa1, 0x2c, 0xaa, 0x12, 0xcf, 0xd1, 0x5e, 0x34, 0xbd, 0x8c, 0x7b, 0x1b, 0x29, 0xf5, 0x68, 0x5d, 0x63, 0xc9, 0x17, 0x50, 0x25, 0x7e, 0x6e, 0xf8, 0x16, 0x02, 0xb7, 0xaf, 0x6d, 0x03, 0x65, 0x0e, 0x2f, 0x0f, 0x20, 0xfe, 0xe1, 0x7e, 0x32, 0x9e, 0x0a, 0xae, 0xb9, 0x34, 0xa3, 0x22, 0x91, 0x06, 0xeb, 0xa8, 0x56, 0x11, 0x26, 0xc4, 0x09, 0x3c, 0xc7, 0xc0, 0xeb};



@implementation ViewController
@synthesize recordButton,voiceSearch;
- (void)viewDidLoad {
    [super viewDidLoad];
    // Do any additional setup after loading the view, typically from a nib.
    
#pragma mark Socket Creation
    //Creation of Socket
    socket = [[GCDAsyncSocket alloc] initWithDelegate:self delegateQueue:dispatch_get_main_queue()];
    
    
    // Fill your ID and Host
    
    [SpeechKit setupWithID:@"NMDPTRIAL_hjdk4_mail_umkc_edu20150910175922"
                      host:@"sandbox.nmdp.nuancemobility.net"
                      port:443
                    useSSL:NO
                  delegate:self];
    SKEarcon* earconStart	= [SKEarcon earconWithName:@"earcon_listening.wav"];
    [SpeechKit setEarcon:earconStart forType:SKStartRecordingEarconType];
    
}


- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
    [socket disconnect];
}

#pragma mark -
#pragma mark UI functions
- (IBAction)tappedOnConnect:(id)sender {
    
#pragma mark Socket Connection
    NSLog(@"Tapped On Connect");
    if (([_ipAddress.text isEqual:NULL])||([_portNumber.text isEqual:NULL])) {
        NSLog(@"IPAddress or Port is Empty");
    }
    else
    {
        NSError *err = nil;
        if (![socket connectToHost:_ipAddress.text onPort:[_portNumber.text intValue] error:&err]) // Asynchronous!
        {
            // If there was an error, it's likely something like "already connected" or "no delegate set"
            NSLog(@"I goofed: %@", err);
        }
        else{
            NSLog(@"connected");
        }
        NSLog(@"%@",_ipAddress.text);
        NSLog(@"%d",[_portNumber.text intValue]);
    }
}

- (IBAction)tappedOnForward:(id)sender {
#pragma mark  Writing into the socket
    [socket writeData:[@"FORWARD" dataUsingEncoding:NSUTF8StringEncoding] withTimeout:-1 tag:1];
}
- (IBAction)tappedOnRight:(id)sender {
    [socket writeData:[@"RIGHT" dataUsingEncoding:NSUTF8StringEncoding] withTimeout:-1 tag:1];
}
- (IBAction)tappedOnBackWard:(id)sender {
    [socket writeData:[@"BACKWARD" dataUsingEncoding:NSUTF8StringEncoding] withTimeout:-1 tag:1];
}
- (IBAction)tappedOnLeft:(id)sender {
    [socket writeData:[@"LEFT" dataUsingEncoding:NSUTF8StringEncoding] withTimeout:-1 tag:1];
}
- (IBAction)tappedOnRecord:(id)sender {
    
    
    if (transactionState == TS_RECORDING) {
        [voiceSearch stopRecording];
    }
    else if (transactionState == TS_IDLE) {
        SKEndOfSpeechDetection detectionType;
        NSString* recoType;
        NSString* langType;
        
        transactionState = TS_INITIAL;
        
        //      alternativesDisplay.text = @"";
        
        /* 'Dictation' is selected */
        detectionType = SKLongEndOfSpeechDetection; /* Dictations tend to be long utterances that may include short pauses. */
        recoType = SKDictationRecognizerType; /* Optimize recognition performance for dictation or message text. */
        langType = @"en_US";
        /* Nuance can also create a custom recognition type optimized for your application if neither search nor dictation are appropriate. */
        
        NSLog(@"Recognizing type:'%@' Language Code: '%@' using end-of-speech detection:%lu.", recoType, langType, (unsigned long)detectionType);
        
        // if (voiceSearch) [voiceSearch release];
        
        voiceSearch = [[SKRecognizer alloc] initWithType:recoType
                                               detection:detectionType
                                                language:langType
                                                delegate:self];

}
}
- (IBAction)tappedOnSendCommand:(id)sender {
    [socket writeData:[_textFromVoice.text dataUsingEncoding:NSUTF8StringEncoding] withTimeout:-1 tag:1];

}

# pragma mark -
# pragma mark GCDAsynSocket delegate methods
- (void)socket:(GCDAsyncSocket *)sender didConnectToHost:(NSString *)host port:(UInt16)port
{
    NSLog(@"Cool, I'm connected! That was easy.");
}

- (void)socket:(GCDAsyncSocket *)sock didWriteDataWithTag:(long)tag
{
    NSLog(@"Data Written");
    
}

- (void)socketDidDisconnect:(GCDAsyncSocket *)sock withError:(NSError *)err
{
    NSLog(@"Disconnected %@",err);
}

# pragma mark -
# pragma mark Voice Recogniser

- (void)recognizer:(SKRecognizer *)recognizer didFinishWithResults:(SKRecognition *)results
{
    NSLog(@"Got results.");
    NSLog(@"Session id [%@].", [SpeechKit sessionID]); // for debugging purpose: printing out the speechkit session id
    
    long numOfResults = [results.results count];
    
    transactionState = TS_IDLE;
    [recordButton setTitle:@"Record" forState:UIControlStateNormal];
    //
    if (numOfResults > 0)
    {
        //Result to Text Box
        [_textFromVoice setText:[results firstResult]];
            }
    if (numOfResults > 1)
    {
        //        alternativesDisplay.text = [[results.results subarrayWithRange:NSMakeRange(1, numOfResults-1)] componentsJoinedByString:@"\n"];
    }
    if (results.suggestion) {
        UIAlertView *alert = [[UIAlertView alloc] initWithTitle:@"Suggestion"
                                                        message:results.suggestion
                                                       delegate:nil
                                              cancelButtonTitle:@"OK"
                                              otherButtonTitles:nil];
        [alert show];
        // [alert release];
        
    }
    
    //  [voiceSearch release];
    // voiceSearch = nil;
}
- (void)recognizer:(SKRecognizer *)recognizer didFinishWithError:(NSError *)error suggestion:(NSString *)suggestion
{
    NSLog(@"Got error.");
    NSLog(@"Session id [%@].", [SpeechKit sessionID]); // for debugging purpose: printing out the speechkit session id
    
    transactionState = TS_IDLE;
    [recordButton setTitle:@"Record" forState:UIControlStateNormal];
    
    UIAlertView *alert = [[UIAlertView alloc] initWithTitle:@"Error"
                                                    message:[error localizedDescription]
                                                   delegate:nil
                                          cancelButtonTitle:@"OK"
                                          otherButtonTitles:nil];
    [alert show];
    // [alert release];
    
    if (suggestion) {
        UIAlertView *alert = [[UIAlertView alloc] initWithTitle:@"Suggestion"
                                                        message:suggestion
                                                       delegate:nil
                                              cancelButtonTitle:@"OK"
                                              otherButtonTitles:nil];
        [alert show];
        //  [alert release];
        
    }
    
    //  [voiceSearch release];
    voiceSearch = nil;
}
- (void)recognizerDidBeginRecording:(SKRecognizer *)recognizer
{
    NSLog(@"Recording started.");
    
    transactionState = TS_RECORDING;
    [recordButton setTitle:@"Recording..." forState:UIControlStateNormal];
    // [self performSelector:@selector(updateVUMeter) withObject:nil afterDelay:0.05];
}

- (void)recognizerDidFinishRecording:(SKRecognizer *)recognizer
{
    NSLog(@"Recording finished.");
    
    //    [NSObject cancelPreviousPerformRequestsWithTarget:self selector:@selector(updateVUMeter) object:nil];
    //
    transactionState = TS_PROCESSING;
    [recordButton setTitle:@"Processing..." forState:UIControlStateNormal];
}

@end
