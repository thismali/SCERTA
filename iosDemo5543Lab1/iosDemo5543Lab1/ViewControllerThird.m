//
//  ViewControllerThird.m
//  iosDemo5543Lab1
//
//  Created by Ali, Mohamoud (UMKC-Student) on 9/13/15.
//  Copyright (c) 2015 Ali, Mohamoud (UMKC-Student). All rights reserved.
//

#import "ViewControllerThird.h"

@interface ViewControllerThird ()
@property (weak, nonatomic) IBOutlet UIWebView *thisWebView;

@end

@implementation ViewControllerThird

- (void)viewDidLoad {
    [super viewDidLoad];
    // Do any additional setup after loading the view.
    
    NSString *URL = @"http://umkc.edu";
    NSURL *url = [NSURL URLWithString:URL];
    NSURLRequest *requestObj = [NSURLRequest requestWithURL:url];
    [_thisWebView  loadRequest:requestObj];
    
}

- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}

/*
#pragma mark - Navigation

// In a storyboard-based application, you will often want to do a little preparation before navigation
- (void)prepareForSegue:(UIStoryboardSegue *)segue sender:(id)sender {
    // Get the new view controller using [segue destinationViewController].
    // Pass the selected object to the new view controller.
}
*/

@end
