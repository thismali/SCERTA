//
//  ViewControllerSecond.m
//  iosDemo5543Lab1
//
//  Created by Ali, Mohamoud (UMKC-Student) on 9/13/15.
//  Copyright (c) 2015 Ali, Mohamoud (UMKC-Student). All rights reserved.
//

#import "ViewControllerSecond.h"
#import "ViewControllerThird.h"

@interface ViewControllerSecond ()

@end

@implementation ViewControllerSecond

- (void)viewDidLoad {
    [super viewDidLoad];
    // Do any additional setup after loading the view.
}

- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}
- (IBAction)handleTouchDown:(id)sender {
    
    
    ViewControllerThird *thirdController = [self.storyboard instantiateViewControllerWithIdentifier:@"ViewControllerThird"];
    
    [self presentViewController:thirdController animated:NO completion:NULL];
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
